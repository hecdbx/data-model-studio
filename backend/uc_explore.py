"""Feature 2 — reverse-engineer an ER model from Unity Catalog.

Browse catalogs/schemas, then reconstruct entities, columns, primary keys and
foreign-key relationships from ``<catalog>.information_schema`` (verified query
pattern: referential_constraints + key_column_usage on both sides).
"""

from __future__ import annotations

import re
import uuid

from .databricks_clients import get_workspace_client, run_sql_dicts, valid_ident
from .model_schema import (
    Column,
    ColumnRef,
    DataModel,
    Entity,
    ModelSource,
    Relationship,
    infer_cardinality,
)

# internal tables created by pipelines (materializations, CDC storage, the
# per-pipeline event log `event_log_<pipeline uuid>`) — hide from the model
_INTERNAL_RE = re.compile(
    r"^(__materialization|_tmp_|__apply_changes|__dlt_"
    r"|event_log_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}$)"
)


def _in_list(schemas: list[str]) -> tuple[str, dict]:
    """Build a parameterized IN (...) clause for schema names."""
    names = [s for s in schemas if valid_ident(s)]
    if not names:
        raise ValueError("No valid schema names provided.")
    markers = ", ".join(f":s{i}" for i in range(len(names)))
    params = {f"s{i}": n for i, n in enumerate(names)}
    return f"({markers})", params


def infer_layer(schema: str, table: str) -> str:
    s, t = schema.lower(), table.lower()
    prefix_map = [
        ("fact_", "fact"), ("dim_", "dimension"), ("bridge_", "bridge"),
        ("hub_", "hub"), ("lnk_", "link"), ("link_", "link"), ("sat_", "satellite"),
        ("bronze_", "bronze"), ("stg_", "bronze"), ("staging_", "bronze"),
        ("silver_", "silver"), ("gold_", "gold"),
    ]
    for pre, layer in prefix_map:
        if t.startswith(pre):
            return layer
    for suf, layer in (("_bronze", "bronze"), ("_silver", "silver"), ("_gold", "gold")):
        if s.endswith(suf):
            return layer
    return "table"


# ─── Browsing ────────────────────────────────────────────────────────────
def list_catalogs() -> list[dict]:
    w = get_workspace_client()
    out = []
    for c in w.catalogs.list(include_browse=True):
        if c.name in ("system", "__databricks_internal"):
            continue
        out.append({"name": c.name, "comment": c.comment or "",
                    "type": c.catalog_type.value if c.catalog_type else ""})
    return sorted(out, key=lambda x: x["name"].lower())


def list_schemas(catalog: str) -> list[dict]:
    if not valid_ident(catalog):
        raise ValueError(f"Invalid catalog: {catalog!r}")
    w = get_workspace_client()
    out = []
    for s in w.schemas.list(catalog_name=catalog):
        if s.name == "information_schema":
            continue
        out.append({"name": s.name, "comment": s.comment or ""})
    return sorted(out, key=lambda x: x["name"])


def schema_table_counts(catalog: str, schemas: list[str]) -> dict[str, int]:
    clause, params = _in_list(schemas)
    rows = run_sql_dicts(
        f"SELECT table_schema, count(*) AS n "
        f"FROM {catalog}.information_schema.tables "
        f"WHERE table_schema IN {clause} GROUP BY table_schema",
        parameters=params,
    ) if valid_ident(catalog) else []
    return {r["table_schema"]: int(r["n"]) for r in rows}


# ─── ER reconstruction ─────────────────────────────────────────────────────
def build_model_from_schemas(catalog: str, schemas: list[str]) -> DataModel:
    if not valid_ident(catalog):
        raise ValueError(f"Invalid catalog: {catalog!r}")
    clause, params = _in_list(schemas)
    ic = f"{catalog}.information_schema"

    # 1) tables
    tables = run_sql_dicts(
        f"SELECT table_schema, table_name, table_type, comment "
        f"FROM {ic}.tables WHERE table_schema IN {clause} ORDER BY table_schema, table_name",
        parameters=params,
    )
    # 2) columns
    columns = run_sql_dicts(
        f"SELECT table_schema, table_name, column_name, full_data_type, is_nullable, "
        f"comment, ordinal_position FROM {ic}.columns "
        f"WHERE table_schema IN {clause} ORDER BY table_schema, table_name, ordinal_position",
        parameters=params,
    )
    # 3) constraints classification
    tcons = run_sql_dicts(
        f"SELECT constraint_schema, constraint_name, table_schema, table_name, constraint_type "
        f"FROM {ic}.table_constraints WHERE constraint_schema IN {clause}",
        parameters=params,
    )
    # 4) key column usage (selected schemas)
    kcu = run_sql_dicts(
        f"SELECT constraint_schema, constraint_name, table_schema, table_name, column_name, "
        f"ordinal_position, position_in_unique_constraint FROM {ic}.key_column_usage "
        f"WHERE constraint_schema IN {clause}",
        parameters=params,
    )
    # 5) referential constraints (FK → unique)
    refc = run_sql_dicts(
        f"SELECT constraint_schema, constraint_name, unique_constraint_schema, unique_constraint_name "
        f"FROM {ic}.referential_constraints WHERE constraint_schema IN {clause}",
        parameters=params,
    )

    # Resolve referenced unique-constraint columns; they may live in schemas we
    # didn't select — fetch those too.
    ref_uq_schemas = {r["unique_constraint_schema"] for r in refc if r.get("unique_constraint_schema")}
    extra = [s for s in ref_uq_schemas if s not in set(schemas) and valid_ident(s)]
    if extra:
        eclause, eparams = _in_list(extra)
        kcu += run_sql_dicts(
            f"SELECT constraint_schema, constraint_name, table_schema, table_name, column_name, "
            f"ordinal_position, position_in_unique_constraint FROM {ic}.key_column_usage "
            f"WHERE constraint_schema IN {eclause}",
            parameters=eparams,
        )

    # ── Index the raw data ──────────────────────────────────────────────
    def eid(schema: str, table: str) -> str:
        return f"{schema}.{table}"

    pk_constraints = {(r["constraint_schema"], r["constraint_name"])
                      for r in tcons if r["constraint_type"] == "PRIMARY KEY"}

    # constraint (schema,name) -> ordered list of (ordinal, table_schema, table, column)
    con_cols: dict[tuple, list] = {}
    for r in kcu:
        key = (r["constraint_schema"], r["constraint_name"])
        con_cols.setdefault(key, []).append((
            int(r["ordinal_position"]),
            r["table_schema"], r["table_name"], r["column_name"],
            r.get("position_in_unique_constraint"),
        ))
    for v in con_cols.values():
        v.sort(key=lambda x: x[0])

    # entity id -> set of PK column names
    pk_by_entity: dict[str, list[str]] = {}
    for key in pk_constraints:
        for _ord, ts, tt, col, _piu in con_cols.get(key, []):
            pk_by_entity.setdefault(eid(ts, tt), []).append(col)

    # ── Build entities ──────────────────────────────────────────────────
    cols_by_entity: dict[str, list[dict]] = {}
    for r in columns:
        cols_by_entity.setdefault(eid(r["table_schema"], r["table_name"]), []).append(r)

    entities: list[Entity] = []
    for t in tables:
        name = t["table_name"]
        if _INTERNAL_RE.match(name):
            continue
        the_id = eid(t["table_schema"], name)
        pk_cols = pk_by_entity.get(the_id, [])
        pkset = set(pk_cols)
        cols = []
        for c in cols_by_entity.get(the_id, []):
            cols.append(Column(
                name=c["column_name"],
                type=(c.get("full_data_type") or "STRING").upper(),
                nullable=(str(c.get("is_nullable", "YES")).upper() != "NO"),
                is_primary_key=c["column_name"] in pkset,
                description=c.get("comment") or "",
            ))
        entities.append(Entity(
            id=the_id, name=name, catalog=catalog, schema=t["table_schema"],
            layer=infer_layer(t["table_schema"], name),
            description=t.get("comment") or "",
            columns=cols, primary_key=pk_cols,
            tags={"table_type": t.get("table_type") or "MANAGED"},
        ))

    entity_ids = {e.id for e in entities}

    # ── Build FK relationships ─────────────────────────────────────────
    relationships: list[Relationship] = []
    fk_by_col: dict[tuple, tuple] = {}  # (entity_id, col) -> (parent_id, parent_col)
    for rc in refc:
        fk_key = (rc["constraint_schema"], rc["constraint_name"])
        uq_key = (rc["unique_constraint_schema"], rc["unique_constraint_name"])
        fk_cols = con_cols.get(fk_key, [])
        uq_cols = con_cols.get(uq_key, [])
        if not fk_cols:
            continue
        # referenced column by position_in_unique_constraint -> uq ordinal_position
        uq_by_pos = {oc[0]: oc for oc in uq_cols}  # ordinal -> tuple
        child_id = eid(fk_cols[0][1], fk_cols[0][2])
        parent_id = eid(uq_cols[0][1], uq_cols[0][2]) if uq_cols else None
        child_columns, parent_columns = [], []
        for _ord, ts, tt, col, piu in fk_cols:
            child_columns.append(col)
            ref = uq_by_pos.get(int(piu)) if piu is not None else None
            if ref:
                parent_columns.append(ref[3])
                fk_by_col[(eid(ts, tt), col)] = (eid(ref[1], ref[2]), ref[3])
        if parent_id is None or parent_id not in entity_ids:
            continue
        relationships.append(Relationship(
            id=f"rel_{rc['constraint_name']}",
            from_entity=child_id, from_columns=child_columns,
            to_entity=parent_id, to_columns=parent_columns,
            type=infer_cardinality(pk_by_entity.get(child_id), child_columns),
            description=f"FK {rc['constraint_name']}",
        ))

    # mark FK columns on entities
    for e in entities:
        for col in e.columns:
            ref = fk_by_col.get((e.id, col.name))
            if ref:
                col.is_foreign_key = True
                col.references = ColumnRef(entity=ref[0], column=ref[1])

    model = DataModel(
        model_type="physical",
        name=f"{catalog}." + (schemas[0] if len(schemas) == 1 else f"{len(schemas)}_schemas"),
        description=f"Reverse-engineered from Unity Catalog: {catalog}.{{{', '.join(schemas)}}}",
        source=ModelSource(kind="uc_schemas", catalog=catalog, schemas=list(schemas)),
        entities=entities,
        relationships=relationships,
    )
    return model
