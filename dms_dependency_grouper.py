#!/usr/bin/env python3
"""
dms_dependency_grouper.py
=========================

Scans a set of Oracle schemas, builds a table-level dependency graph from
FOREIGN KEY -> PRIMARY/UNIQUE KEY relationships (including cross-schema ones),
computes connected components, and packs those components into AWS DMS tasks
such that a component is NEVER split across two tasks.

Outputs
-------
1. <out>/dms_task_plan.xlsx   - Tables, FK_Edges, Components, Task_Assignments, Summary
2. <out>/mappings/<task>.json - DMS table-mapping JSON, one file per task

Usage
-----
    pip install oracledb pandas openpyxl

    # Edit the CONFIG block below, then:
    python dms_dependency_grouper.py

Notes
-----
* Requires SELECT on ALL_TABLES / ALL_CONSTRAINTS (DBA_SEGMENTS optional, for sizing).
* Parent tables living OUTSIDE --schemas are reported in the Summary sheet as
  "external parents" -- they are a real risk and are not silently swallowed.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

import oracledb
import pandas as pd

Table = Tuple[str, str]  # (owner, table_name)


# ============================================================================
# CONFIG -- edit this block, then just run: python dms_dependency_grouper.py
# ============================================================================
CONFIG = {
    # --- Oracle source connection -------------------------------------------
    "dsn": "host:1521/ORCLPDB1",
    "user": "migr_reader",
    # Leave PASSWORD as None to pull from the ORACLE_PASSWORD env var instead
    # of hardcoding it. Hardcode only for throwaway/local runs.
    "password": None,

    # --- Scope ---------------------------------------------------------------
    "schemas": ["SALES", "HR", "FIN", "STAGING"],

    # Tables to exclude from the graph entirely, as "OWNER.TABLE".
    # Use this for shared hub/lookup tables that otherwise collapse every
    # component into one giant cluster. Supports a trailing % wildcard.
    "exclude_tables": [
        # "FIN.AUDIT_LOG",
        # "SALES.TMP_%",
    ],

    # --- Task packing --------------------------------------------------------
    "max_tables_per_task": 40,
    "task_prefix": "dms",

    # --- Target / output -----------------------------------------------------
    "lowercase": True,          # emit convert-lowercase rules for the PG target
    "out_dir": "./dms_plan",
}
# ============================================================================


# ----------------------------------------------------------------------------
# Union-Find
# ----------------------------------------------------------------------------
class UnionFind:
    def __init__(self) -> None:
        self.parent: Dict[Table, Table] = {}
        self.rank: Dict[Table, int] = {}

    def add(self, x: Table) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: Table) -> Table:
        self.add(x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:  # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: Table, b: Table) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1

    def groups(self) -> Dict[Table, List[Table]]:
        out: Dict[Table, List[Table]] = defaultdict(list)
        for node in self.parent:
            out[self.find(node)].append(node)
        return out


# ----------------------------------------------------------------------------
# Oracle metadata extraction
# ----------------------------------------------------------------------------
SQL_TABLES = """
SELECT t.owner, t.table_name, NVL(t.num_rows, 0) AS num_rows, t.partitioned
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
SELECT c.owner            AS child_owner,
       c.table_name       AS child_table,
       c.constraint_name  AS fk_name,
       p.owner            AS parent_owner,
       p.table_name       AS parent_table,
       c.delete_rule
FROM   all_constraints c
JOIN   all_constraints p
       ON  p.owner           = c.r_owner
       AND p.constraint_name = c.r_constraint_name
WHERE  c.constraint_type = 'R'
  AND  c.status          = 'ENABLED'
  AND  (c.owner IN ({binds}) OR p.owner IN ({binds2}))
"""

SQL_SIZES = """
SELECT owner, segment_name, SUM(bytes) AS bytes
FROM   dba_segments
WHERE  owner IN ({binds})
  AND  segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
GROUP  BY owner, segment_name
"""


def _bind_list(prefix: str, n: int) -> str:
    return ", ".join(f":{prefix}{i}" for i in range(n))


def fetch_tables(cur, schemas: List[str]) -> pd.DataFrame:
    sql = SQL_TABLES.format(binds=_bind_list("s", len(schemas)))
    cur.execute(sql, {f"s{i}": s for i, s in enumerate(schemas)})
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["owner", "table_name", "num_rows", "partitioned"])


def fetch_fks(cur, schemas: List[str]) -> pd.DataFrame:
    sql = SQL_FKS.format(
        binds=_bind_list("s", len(schemas)),
        binds2=_bind_list("s", len(schemas)),
    )
    cur.execute(sql, {f"s{i}": s for i, s in enumerate(schemas)})
    rows = cur.fetchall()
    return pd.DataFrame(
        rows,
        columns=["child_owner", "child_table", "fk_name",
                 "parent_owner", "parent_table", "delete_rule"],
    )


def fetch_sizes(cur, schemas: List[str]) -> Dict[Table, int]:
    """Best-effort; returns {} if DBA_SEGMENTS is not visible."""
    try:
        sql = SQL_SIZES.format(binds=_bind_list("s", len(schemas)))
        cur.execute(sql, {f"s{i}": s for i, s in enumerate(schemas)})
        return {(o, n): int(b or 0) for o, n, b in cur.fetchall()}
    except oracledb.DatabaseError as exc:
        print(f"[warn] DBA_SEGMENTS unavailable, sizing skipped: {exc}", file=sys.stderr)
        return {}


# ----------------------------------------------------------------------------
# Graph build + component detection
# ----------------------------------------------------------------------------
@dataclass
class Component:
    comp_id: int
    tables: List[Table]
    kind: str                       # 'dependency' | 'standalone'
    self_ref_tables: List[Table] = field(default_factory=list)
    est_rows: int = 0
    est_bytes: int = 0
    task_name: str = ""

    @property
    def size(self) -> int:
        return len(self.tables)

    @property
    def schemas(self) -> List[str]:
        return sorted({o for o, _ in self.tables})


def build_components(
    tables: Set[Table],
    fks: pd.DataFrame,
    in_scope: Set[str],
) -> Tuple[List[Component], List[dict]]:
    uf = UnionFind()
    for t in tables:
        uf.add(t)

    self_ref: Set[Table] = set()
    external_parents: List[dict] = []

    for row in fks.itertuples(index=False):
        child = (row.child_owner, row.child_table)
        parent = (row.parent_owner, row.parent_table)

        if row.parent_owner not in in_scope or row.child_owner not in in_scope:
            external_parents.append({
                "fk_name": row.fk_name,
                "child": f"{child[0]}.{child[1]}",
                "parent": f"{parent[0]}.{parent[1]}",
                "out_of_scope_side": "parent" if row.parent_owner not in in_scope else "child",
            })
            continue

        if child == parent:
            self_ref.add(child)
            continue

        uf.union(child, parent)

    comps: List[Component] = []
    for cid, (_, members) in enumerate(
        sorted(uf.groups().items(), key=lambda kv: (-len(kv[1]), kv[0])), start=1
    ):
        members_sorted = sorted(members)
        comps.append(
            Component(
                comp_id=cid,
                tables=members_sorted,
                kind="dependency" if len(members_sorted) > 1 else "standalone",
                self_ref_tables=[t for t in members_sorted if t in self_ref],
            )
        )
    return comps, external_parents


# ----------------------------------------------------------------------------
# Bin packing: components -> tasks (first-fit-decreasing, components never split)
# ----------------------------------------------------------------------------
def pack(
    comps: List[Component],
    max_tables: int,
    prefix: str,
) -> None:
    dep = sorted([c for c in comps if c.kind == "dependency"],
                 key=lambda c: -c.size)
    solo = sorted([c for c in comps if c.kind == "standalone"],
                  key=lambda c: -c.size)

    def _fit(group: List[Component], label: str) -> None:
        bins: List[List[Component]] = []
        loads: List[int] = []
        for c in group:
            placed = False
            # A component larger than max_tables gets its own oversized task.
            if c.size >= max_tables:
                bins.append([c])
                loads.append(c.size)
                placed = True
            else:
                for i, load in enumerate(loads):
                    if load + c.size <= max_tables:
                        bins[i].append(c)
                        loads[i] += c.size
                        placed = True
                        break
            if not placed:
                bins.append([c])
                loads.append(c.size)
        for i, b in enumerate(bins, start=1):
            name = f"{prefix}-{label}-{i:03d}"
            for c in b:
                c.task_name = name

    _fit(dep, "dep")
    _fit(solo, "solo")


# ----------------------------------------------------------------------------
# DMS table-mapping JSON
# ----------------------------------------------------------------------------
def build_mapping(tables: List[Table], lowercase: bool) -> dict:
    rules: List[dict] = []
    rid = 1
    for owner, tab in sorted(tables):
        rules.append({
            "rule-type": "selection",
            "rule-id": str(rid),
            "rule-name": str(rid),
            "object-locator": {"schema-name": owner, "table-name": tab},
            "rule-action": "include",
            "filters": [],
        })
        rid += 1

    if lowercase:
        rid = 1000
        for owner in sorted({o for o, _ in tables}):
            rules.append({
                "rule-type": "transformation",
                "rule-id": str(rid),
                "rule-name": str(rid),
                "rule-target": "schema",
                "object-locator": {"schema-name": owner},
                "rule-action": "convert-lowercase",
            })
            rid += 1
            rules.append({
                "rule-type": "transformation",
                "rule-id": str(rid),
                "rule-name": str(rid),
                "rule-target": "table",
                "object-locator": {"schema-name": owner, "table-name": "%"},
                "rule-action": "convert-lowercase",
            })
            rid += 1
            rules.append({
                "rule-type": "transformation",
                "rule-id": str(rid),
                "rule-name": str(rid),
                "rule-target": "column",
                "object-locator": {"schema-name": owner, "table-name": "%", "column-name": "%"},
                "rule-action": "convert-lowercase",
            })
            rid += 1

    return {"rules": rules}


# ----------------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------------
def write_excel(
    path: str,
    tbl_df: pd.DataFrame,
    fk_df: pd.DataFrame,
    comps: List[Component],
    external_parents: List[dict],
    sizes: Dict[Table, int],
) -> None:
    comp_rows, assign_rows = [], []
    for c in comps:
        comp_rows.append({
            "component_id": c.comp_id,
            "task_name": c.task_name,
            "kind": c.kind,
            "table_count": c.size,
            "schemas": ",".join(c.schemas),
            "cross_schema": "Y" if len(c.schemas) > 1 else "N",
            "self_ref_tables": ",".join(f"{o}.{t}" for o, t in c.self_ref_tables),
            "est_rows": c.est_rows,
            "est_mb": round(c.est_bytes / 1024 / 1024, 2),
        })
        for owner, tab in c.tables:
            assign_rows.append({
                "task_name": c.task_name,
                "component_id": c.comp_id,
                "kind": c.kind,
                "owner": owner,
                "table_name": tab,
                "est_rows": int(tbl_df.loc[
                    (tbl_df.owner == owner) & (tbl_df.table_name == tab), "num_rows"
                ].sum()),
                "est_mb": round(sizes.get((owner, tab), 0) / 1024 / 1024, 2),
            })

    comp_df = pd.DataFrame(comp_rows)
    assign_df = pd.DataFrame(assign_rows).sort_values(["task_name", "owner", "table_name"])

    summary = (
        assign_df.groupby("task_name")
        .agg(tables=("table_name", "count"),
             components=("component_id", "nunique"),
             est_rows=("est_rows", "sum"),
             est_mb=("est_mb", "sum"))
        .reset_index()
    )

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        summary.to_excel(xw, sheet_name="Summary", index=False)
        assign_df.to_excel(xw, sheet_name="Task_Assignments", index=False)
        comp_df.to_excel(xw, sheet_name="Components", index=False)
        fk_df.to_excel(xw, sheet_name="FK_Edges", index=False)
        tbl_df.to_excel(xw, sheet_name="Tables", index=False)
        pd.DataFrame(external_parents or [{"note": "none"}]).to_excel(
            xw, sheet_name="External_Parents", index=False
        )


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def is_excluded(t: Table, patterns: List[str]) -> bool:
    """Match 'OWNER.TABLE', with optional trailing % wildcard."""
    full = f"{t[0]}.{t[1]}"
    for p in patterns:
        p = p.strip().upper()
        if p.endswith("%"):
            if full.startswith(p[:-1]):
                return True
        elif full == p:
            return True
    return False


def main() -> int:
    dsn = CONFIG["dsn"]
    user = CONFIG["user"]
    password = CONFIG["password"] or os.getenv("ORACLE_PASSWORD")
    if not password:
        print("[fatal] no password: set CONFIG['password'] or the "
              "ORACLE_PASSWORD env var", file=sys.stderr)
        return 2

    schemas = [s.strip().upper() for s in CONFIG["schemas"] if s.strip()]
    in_scope = set(schemas)
    excludes = [p.upper() for p in CONFIG.get("exclude_tables", [])]
    max_tables = int(CONFIG["max_tables_per_task"])
    prefix = CONFIG["task_prefix"]
    lowercase = bool(CONFIG["lowercase"])
    out_dir = CONFIG["out_dir"]

    os.makedirs(os.path.join(out_dir, "mappings"), exist_ok=True)

    with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
        cur = conn.cursor()
        cur.arraysize = 5000
        print(f"[info] scanning {len(schemas)} schema(s): {', '.join(schemas)}")
        tbl_df = fetch_tables(cur, schemas)
        fk_df = fetch_fks(cur, schemas)
        sizes = fetch_sizes(cur, schemas)

    if excludes:
        before = len(tbl_df)
        keep = [not is_excluded((r.owner, r.table_name), excludes)
                for r in tbl_df.itertuples(index=False)]
        tbl_df = tbl_df[keep].reset_index(drop=True)
        fk_keep = [
            not (is_excluded((r.child_owner, r.child_table), excludes)
                 or is_excluded((r.parent_owner, r.parent_table), excludes))
            for r in fk_df.itertuples(index=False)
        ]
        fk_df = fk_df[fk_keep].reset_index(drop=True)
        print(f"[info] excluded {before - len(tbl_df)} table(s) via exclude_tables")

    tables: Set[Table] = {(r.owner, r.table_name) for r in tbl_df.itertuples(index=False)}
    print(f"[info] {len(tables)} tables, {len(fk_df)} enabled FK constraints")

    comps, external_parents = build_components(tables, fk_df, in_scope)

    row_lookup = {(r.owner, r.table_name): int(r.num_rows)
                  for r in tbl_df.itertuples(index=False)}
    for c in comps:
        c.est_rows = sum(row_lookup.get(t, 0) for t in c.tables)
        c.est_bytes = sum(sizes.get(t, 0) for t in c.tables)

    pack(comps, max_tables, prefix)

    for task_name in sorted({c.task_name for c in comps}):
        task_tables = [t for c in comps if c.task_name == task_name for t in c.tables]
        mapping = build_mapping(task_tables, lowercase)
        fname = re.sub(r"[^A-Za-z0-9_.-]", "_", task_name) + ".json"
        with open(os.path.join(out_dir, "mappings", fname), "w") as fh:
            json.dump(mapping, fh, indent=2)

    xlsx = os.path.join(out_dir, "dms_task_plan.xlsx")
    write_excel(xlsx, tbl_df, fk_df, comps, external_parents, sizes)

    dep_c = [c for c in comps if c.kind == "dependency"]
    solo_c = [c for c in comps if c.kind == "standalone"]
    print(f"[info] {len(dep_c)} dependency clusters "
          f"({sum(c.size for c in dep_c)} tables), "
          f"{len(solo_c)} standalone tables")
    print(f"[info] {len({c.task_name for c in comps})} DMS tasks planned")
    if external_parents:
        print(f"[WARN] {len(external_parents)} FK(s) cross the scan boundary "
              f"-- see External_Parents sheet")
    print(f"[info] report: {xlsx}")
    print(f"[info] mappings: {os.path.join(out_dir, 'mappings')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
