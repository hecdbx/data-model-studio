"""Deterministic code generation from a :class:`DataModel`.

Two artifact families, matching the product promise ("give the DML for it or an
updated Lakeflow pipeline code"):

- ``generate_ddl`` / ``generate_migration`` — Unity Catalog SQL (CREATE TABLE with
  PK/FK constraints; additive ALTER migration for edits).
- ``generate_sdp`` — a Spark Declarative Pipeline ``pipeline.py`` scaffold using the
  modern ``from pyspark import pipelines as dp`` API.

These run without the agent (instant, reliable). The agent can refine them or
reason about non-additive changes.
"""

from __future__ import annotations

import re
from typing import Optional

from .model_schema import DataModel, Entity

# Databricks SQL type normalization (uppercased keys).
_TYPE_MAP = {
    "INTEGER": "INT", "INT": "INT", "LONG": "BIGINT", "BIGINT": "BIGINT",
    "SHORT": "SMALLINT", "SMALLINT": "SMALLINT", "BYTE": "TINYINT", "TINYINT": "TINYINT",
    "STRING": "STRING", "TEXT": "STRING", "VARCHAR": "STRING", "CHAR": "STRING",
    "DOUBLE": "DOUBLE", "FLOAT": "FLOAT", "REAL": "FLOAT",
    "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    "BINARY": "BINARY",
}

_LAYER_ORDER = {
    "bronze": 0, "hub": 0, "staging": 0,
    "silver": 1, "link": 1, "satellite": 1, "dimension": 1, "bridge": 1,
    "gold": 2, "fact": 2,
    "table": 3,
}


def _q(ident: str) -> str:
    """Backtick-quote an identifier (escaping embedded backticks)."""
    return "`" + str(ident).replace("`", "``") + "`"


def _fqn(catalog: str, schema: str, table: str) -> str:
    return f"{_q(catalog)}.{_q(schema)}.{_q(table)}"


def _esc(s: str) -> str:
    return str(s or "").replace("'", "''")


def _sanitize(name: str, maxlen: int = 60) -> str:
    s = re.sub(r"[^A-Za-z0-9_]", "_", str(name)).strip("_") or "c"
    return s[:maxlen]


def _norm_type(t: str) -> str:
    t = (t or "STRING").strip()
    up = t.upper()
    if up.startswith("DECIMAL") or up.startswith("NUMERIC"):
        return up.replace("NUMERIC", "DECIMAL")
    if up.startswith("ARRAY") or up.startswith("MAP") or up.startswith("STRUCT"):
        return up
    return _TYPE_MAP.get(up, up)


def _pk_cols(entity: Entity) -> list[str]:
    if entity.primary_key:
        return list(entity.primary_key)
    return [c.name for c in entity.columns if c.is_primary_key]


def _foreign_keys(model: DataModel):
    """Yield (child_entity, child_cols, parent_entity, parent_cols), de-duplicated.

    Relationships are authoritative. Column-level ``references`` only add FKs a
    relationship doesn't already cover; references into a composite key are
    grouped into one composite FK (a partial reference can't be a valid FK)."""
    seen = set()
    covered = set()  # (child id, column) already in a relationship

    def parent_pk(name: str) -> list[str]:
        e = model.entity_by_id(name)
        return _pk_cols(e) if e else []

    for rel in model.relationships:
        child = model.entity_by_id(rel.from_entity)
        parent = model.entity_by_id(rel.to_entity)
        if not child or not parent:
            continue
        child_cols = rel.from_columns or []
        parent_cols = rel.to_columns or parent_pk(parent.name)
        if not child_cols or not parent_cols or len(child_cols) != len(parent_cols):
            continue
        covered.update((child.id, c) for c in child_cols)
        key = (child.name, tuple(child_cols), parent.name, tuple(parent_cols))
        if key in seen:
            continue
        seen.add(key)
        yield child, child_cols, parent, parent_cols

    for e in model.entities:
        by_parent: dict[str, list] = {}
        for col in e.columns:
            if (col.references and col.references.entity and col.references.column
                    and (e.id, col.name) not in covered):
                by_parent.setdefault(col.references.entity, []).append((col.name, col.references.column))
        for parent_ref, pairs in by_parent.items():
            parent = model.entity_by_id(parent_ref)
            if not parent:
                continue
            pk = _pk_cols(parent)
            if len(pk) > 1:
                ref = {pc: cc for cc, pc in pairs}
                groups = [[(ref[p], p) for p in pk]] if all(p in ref for p in pk) else []
            else:
                groups = [[p] for p in pairs]
            for g in groups:
                cc, pc = [x[0] for x in g], [x[1] for x in g]
                key = (e.name, tuple(cc), parent.name, tuple(pc))
                if key in seen:
                    continue
                seen.add(key)
                yield e, cc, parent, pc



def _fk_problem(parent: Entity, parent_cols: list[str]) -> Optional[str]:
    """Why an FK can't be created in Unity Catalog, or None if it can: the
    referenced columns must be exactly the parent's primary key."""
    pk = _pk_cols(parent)
    if not pk:
        return f"{parent.name} has no primary key"
    if set(parent_cols) != set(pk):
        return f"{', '.join(parent_cols)} is not the primary key of {parent.name} ({', '.join(pk)})"
    return None


# ─────────────────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────────────────
def generate_ddl(model: DataModel, catalog: str, schema: str) -> str:
    def cs(e: Entity) -> tuple[str, str]:
        return (e.catalog or catalog, e.schema_name or schema)

    def efqn(e: Entity) -> str:
        c, s = cs(e)
        return _fqn(c, s, e.name)

    lines: list[str] = [
        "-- ============================================================",
        f"-- Data Model Studio — Unity Catalog DDL",
        f"-- Model: {model.name}  ({model.model_type})",
        f"-- Default target (tables without their own schema): {catalog}.{schema}",
        "-- Idempotent: safe to re-run (CREATE ... IF NOT EXISTS,",
        "--   constraints dropped-if-exists then re-added).",
        "-- ============================================================",
        "",
    ]

    # Ensure every distinct schema referenced by the model exists.
    distinct = []
    for e in model.entities:
        pair = cs(e)
        if pair not in distinct:
            distinct.append(pair)
    if not distinct:
        distinct = [(catalog, schema)]
    for c, s in distinct:
        lines.append(f"CREATE SCHEMA IF NOT EXISTS {_q(c)}.{_q(s)};")
    lines.append("")

    ordered = sorted(model.entities, key=lambda e: _LAYER_ORDER.get(e.layer or "table", 3))

    # 1) Tables
    for e in ordered:
        pk = set(_pk_cols(e))
        lines.append(f"-- {e.name}" + (f" — {e.description}" if e.description else ""))
        col_defs = []
        for col in e.columns:
            typ = _norm_type(col.type)
            not_null = " NOT NULL" if (col.name in pk or not col.nullable) else ""
            comment = f" COMMENT '{_esc(col.description)}'" if col.description else ""
            col_defs.append(f"  {_q(col.name)} {typ}{not_null}{comment}")
        cols_block = ",\n".join(col_defs) if col_defs else "  -- (no columns defined)"
        stmt = f"CREATE TABLE IF NOT EXISTS {efqn(e)} (\n{cols_block}\n) USING DELTA"
        if e.description:
            stmt += f"\nCOMMENT '{_esc(e.description)}'"
        if e.partition_by:
            stmt += f"\nPARTITIONED BY ({', '.join(_q(c) for c in e.partition_by)})"
        elif e.cluster_by:
            stmt += f"\nCLUSTER BY ({', '.join(_q(c) for c in e.cluster_by)})"
        props = dict(e.tags or {})
        if e.layer:
            props.setdefault("layer", e.layer)
        if props:
            kv = ", ".join(f"'{_esc(k)}' = '{_esc(v)}'" for k, v in props.items())
            stmt += f"\nTBLPROPERTIES ({kv})"
        lines.append(stmt + ";")
        lines.append("")

    # 2) Primary keys
    pk_emitted = False
    for e in ordered:
        pk = _pk_cols(e)
        if not pk:
            continue
        pk_emitted = True
        cname = _sanitize(f"pk_{e.name}")
        cols = ", ".join(_q(c) for c in pk)
        lines.append(f"ALTER TABLE {efqn(e)} DROP CONSTRAINT IF EXISTS {_q(cname)};")
        lines.append(
            f"ALTER TABLE {efqn(e)} ADD CONSTRAINT {_q(cname)} PRIMARY KEY ({cols});"
        )
    if pk_emitted:
        lines.append("")

    # 3) Foreign keys (after all PKs exist)
    fk_any = False
    for child, child_cols, parent, parent_cols in _foreign_keys(model):
        fk_any = True
        problem = _fk_problem(parent, parent_cols)
        if problem:
            lines.append(f"-- skipped FK {child.name}({', '.join(child_cols)}) -> {parent.name}: {problem}")
            continue
        cname = _sanitize(f"fk_{child.name}_{'_'.join(child_cols)}")
        cc = ", ".join(_q(c) for c in child_cols)
        pc = ", ".join(_q(c) for c in parent_cols)
        lines.append(f"ALTER TABLE {efqn(child)} DROP CONSTRAINT IF EXISTS {_q(cname)};")
        lines.append(
            f"ALTER TABLE {efqn(child)} ADD CONSTRAINT {_q(cname)} "
            f"FOREIGN KEY ({cc}) REFERENCES {efqn(parent)} ({pc});"
        )
    if fk_any:
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


# ─────────────────────────────────────────────────────────────────────────
# Additive migration (edited model vs. the version loaded from UC)
# ─────────────────────────────────────────────────────────────────────────
def generate_migration(base: DataModel, edited: DataModel, catalog: str, schema: str) -> str:
    """Emit deterministic, *additive* SQL for the diff base → edited: new tables,
    new columns, new PK/FK. Destructive changes (drops, type changes) are flagged
    as commented warnings rather than executed — the agent handles those safely."""
    lines: list[str] = [
        "-- ============================================================",
        "-- Data Model Studio — additive migration (edits → SQL)",
        f"-- Default target (tables without their own schema): {catalog}.{schema}",
        "-- Only additive changes are emitted. Review flagged items by hand.",
        "-- ============================================================",
        "",
    ]
    def efqn(e: Entity) -> str:
        return _fqn(e.catalog or catalog, e.schema_name or schema, e.name)

    base_by_name = {e.name: e for e in base.entities}
    warnings: list[str] = []

    new_tables = [e for e in edited.entities if e.name not in base_by_name]
    if new_tables:
        lines.append("-- New tables")
        sub = DataModel(model_type=edited.model_type, name=edited.name,
                        entities=new_tables, relationships=[])
        lines.append(generate_ddl(sub, catalog, schema))

    for e in edited.entities:
        be = base_by_name.get(e.name)
        if not be:
            continue
        base_cols = {c.name: c for c in be.columns}
        for col in e.columns:
            bc = base_cols.get(col.name)
            if bc is None:
                typ = _norm_type(col.type)
                comment = f" COMMENT '{_esc(col.description)}'" if col.description else ""
                lines.append(
                    f"ALTER TABLE {efqn(e)} "
                    f"ADD COLUMN {_q(col.name)} {typ}{comment};"
                )
            elif _norm_type(bc.type) != _norm_type(col.type):
                warnings.append(
                    f"-- CHANGED TYPE {e.name}.{col.name}: {_norm_type(bc.type)} -> {_norm_type(col.type)} "
                    f"(type changes are not applied automatically)"
                )
        edited_cols = {c.name for c in e.columns}
        for bc_name in base_cols:
            if bc_name not in edited_cols:
                warnings.append(f"-- DROPPED COLUMN {e.name}.{bc_name} (not applied automatically)")

    # New foreign keys
    base_fks = {(c.name, tuple(cc), p.name, tuple(pc)) for c, cc, p, pc in _foreign_keys(base)}
    fk_lines = []
    for child, child_cols, parent, parent_cols in _foreign_keys(edited):
        key = (child.name, tuple(child_cols), parent.name, tuple(parent_cols))
        if key in base_fks:
            continue
        problem = _fk_problem(parent, parent_cols)
        if problem:
            warnings.append(f"-- SKIPPED FK {child.name}({', '.join(child_cols)}) -> {parent.name}: {problem}")
            continue
        cname = _sanitize(f"fk_{child.name}_{'_'.join(child_cols)}")
        cc = ", ".join(_q(c) for c in child_cols)
        pc = ", ".join(_q(c) for c in parent_cols)
        fk_lines.append(f"ALTER TABLE {efqn(child)} DROP CONSTRAINT IF EXISTS {_q(cname)};")
        fk_lines.append(
            f"ALTER TABLE {efqn(child)} ADD CONSTRAINT {_q(cname)} "
            f"FOREIGN KEY ({cc}) REFERENCES {efqn(parent)} ({pc});"
        )
    if fk_lines:
        lines.append("")
        lines.append("-- New foreign keys")
        lines.extend(fk_lines)

    if warnings:
        lines.append("")
        lines.append("-- ⚠ Review — not applied automatically:")
        lines.extend(warnings)

    emitted = any(l and not l.lstrip().startswith("--") for l in lines)
    body = "\n".join(lines).rstrip()
    if not emitted:
        body += "\n\n-- No additive changes detected."
    return body + "\n"


# ─────────────────────────────────────────────────────────────────────────
# Spark Declarative Pipeline (Lakeflow) scaffold
# ─────────────────────────────────────────────────────────────────────────
_SOURCE_RANK = {"bronze": 0, "staging": 0, "hub": 1, "link": 1, "satellite": 1, "silver": 1,
                "table": 1, "dimension": 2, "fact": 2, "bridge": 2, "gold": 2}
_LAYER_PREFIX = re.compile(r"^(?:bronze|silver|gold|stg|staging|raw|dim|fact|bridge|hub|sat|lnk|link)_", re.I)
_DATE_DIMS = {"dim_date", "date_dim", "dim_calendar", "calendar", "dates", "dim_day", "dim_dates"}


def _base_name(name: str) -> str:
    n = _LAYER_PREFIX.sub("", name.lower())
    if n.endswith("ies") and len(n) > 4:
        return n[:-3] + "y"
    if n.endswith("s") and not n.endswith("ss") and len(n) > 3:
        return n[:-1]
    return n


def _rank(e: Entity) -> int:
    return _SOURCE_RANK.get(e.layer or "table", 1)


def _upstreams(model: DataModel, entity: Entity) -> list[Entity]:
    """Where an entity's data comes from, most likely first: pipeline lineage,
    else the same-named entity one layer down (silver_sites ← bronze_sites,
    dim_site ← silver_sites). FK parents are join partners, not data sources."""
    ups = []
    for l in model.lineage:
        if l.get("target") == entity.name:
            u = model.entity_by_id(l.get("source") or "")
            if u and u is not entity and u not in ups:
                ups.append(u)
    if ups:
        return ups
    base, rank = _base_name(entity.name), _rank(entity)
    below = [e for e in model.entities if e is not entity and _base_name(e.name) == base and _rank(e) < rank]
    return sorted(below, key=_rank, reverse=True)[:1]


def _spark_type(t: str) -> str:
    return _norm_type(t).lower()


def _pk_expr(col: str, typ: str, up_cols: dict) -> str:
    """A surrogate/natural key column missing upstream: hash the matching natural
    id (site_key ← xxhash64(site_id)) so dimension and fact keys line up."""
    stem = re.sub(r"_(key|sk|id|hk|hash_key)$", "", col.lower())
    for cand in (f"{stem}_id", f"{stem}_code", f"{stem}_no", stem, "id"):
        if cand in up_cols and cand != col.lower():
            return f'F.xxhash64(F.col("{up_cols[cand]}")).cast("{_spark_type(typ)}").alias("{col}")'
    return f'F.monotonically_increasing_id().cast("{_spark_type(typ)}").alias("{col}")'


def _projection(entity: Entity, upstream: Entity) -> tuple[list[str], list[str]]:
    """Select expressions mapping the upstream onto the modeled columns: carried
    columns are cast to the modeled type, missing keys are derived, anything
    else missing becomes a typed NULL flagged with a TODO."""
    up_cols = {c.name.lower(): c.name for c in upstream.columns}
    exprs, todo = [], []
    pk = set(_pk_cols(entity))
    for c in entity.columns:
        t = _spark_type(c.type)
        surrogate = re.search(r"_(key|sk|hk|hash_key)$", c.name.lower())
        if c.name.lower() in up_cols:
            exprs.append(f'F.col("{up_cols[c.name.lower()]}").cast("{t}").alias("{c.name}")')
        elif c.name.lower() in ("_ingested_at", "_processed_at", "_updated_at", "_loaded_at"):
            exprs.append(f'F.current_timestamp().alias("{c.name}")')
        elif c.name in pk:
            exprs.append(_pk_expr(c.name, c.type, up_cols))
        elif surrogate and "monotonically" not in _pk_expr(c.name, c.type, up_cols):
            # a surrogate / foreign surrogate key with a natural id upstream: hash it
            exprs.append(_pk_expr(c.name, c.type, up_cols))
        else:
            exprs.append(f'F.lit(None).cast("{t}").alias("{c.name}")')
            todo.append(c.name)
    return exprs, todo


def _ddl_schema(entity: Entity) -> str:
    return ", ".join(f"{c.name} {_norm_type(c.type)}" for c in entity.columns) or "id BIGINT"


def generate_sdp(model: DataModel, catalog: str, schema: str,
                 source_volume: Optional[str] = None) -> str:
    """A Spark Declarative Pipeline that runs as generated: bronze/source tables
    ingest their dataset from the volume with Auto Loader; every downstream table
    reads its upstream with ``spark.read.table`` and projects the modeled columns.
    What can't be derived is marked TODO (typed NULLs / an empty typed table)."""
    ordered = sorted(model.entities, key=lambda e: _LAYER_ORDER.get(e.layer or "table", 3))
    src = source_volume or f"/Volumes/{catalog}/{schema}/landing"
    from_volume = bool(source_volume) or (model.source and model.source.kind == "volume")

    out: list[str] = [
        "# Databricks notebook source",
        '"""Spark Declarative Pipeline generated by Data Model Studio.',
        "",
        f"Model: {model.name} ({model.model_type}) → target {catalog}.{schema}",
        "Source tables ingest raw files from SOURCE_VOLUME with Auto Loader; downstream",
        "tables read their upstream and project the modeled columns. Search for TODO to",
        "add business logic (joins, derivations) the model alone can't express.",
        '"""',
        "from pyspark import pipelines as dp",
        "from pyspark.sql import functions as F",
        "",
        f'CATALOG = spark.conf.get("target_catalog", "{catalog}")',
        f'SCHEMA = spark.conf.get("target_schema", "{schema}")',
        f'SOURCE_VOLUME = spark.conf.get("source_volume_path", "{src}")',
        "",
    ]

    for e in ordered:
        pk = _pk_cols(e)
        ups = _upstreams(model, e)
        source_path = getattr(e, "source_path", None) or (e.tags or {}).get("source_path")
        source_fmt = (getattr(e, "source_format", None) or (e.tags or {}).get("source_format") or "csv").lower()
        is_date_dim = e.name.lower() in _DATE_DIMS
        is_source = not ups and not is_date_dim and (
            bool(source_path) or _rank(e) == 0 or (from_volume and _rank(e) <= 1))

        deco = "dp.table" if is_source else "dp.materialized_view"
        lines = [f"@{deco}(", f'    name="{e.name}",']
        if e.description:
            lines.append(f'    comment="{_esc(e.description)[:240]}",')
        lines.append(")")
        out.append("\n".join(lines))
        for c in pk:
            out.append(f'@dp.expect_or_drop("valid_{_sanitize(c)}", "{c} IS NOT NULL")')
        out.append(f"def {_sanitize(e.name)}():")

        if is_source:
            dataset = (source_path or _LAYER_PREFIX.sub("", e.name)).strip("/")
            fmt = source_fmt if source_fmt in ("csv", "json", "parquet", "avro", "orc", "text") else "csv"
            out.append(f'    path = f"{{SOURCE_VOLUME}}/{dataset}"')
            out.append("    df = (")
            out.append('        spark.readStream.format("cloudFiles")')
            out.append(f'        .option("cloudFiles.format", "{fmt}")')
            if fmt == "csv":
                out.append('        .option("header", "true")')
            out.append('        .option("cloudFiles.inferColumnTypes", "true")')
            out.append("        .load(path)")
            out.append("    )")
            names = {c.name.lower() for c in e.columns}
            if "_ingested_at" in names or not names:
                out.append('    df = df.withColumn("_ingested_at", F.current_timestamp())')
            for meta in ("_source_file", "_source_path", "_file_path"):
                if meta in names:
                    out.append(f'    df = df.withColumn("{meta}", F.col("_metadata.file_path"))')
            if "_batch_id" in names:
                out.append('    df = df.withColumn("_batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))')
            out.append("    return df")
        elif ups:
            primary = ups[0]
            out.append(f'    df = spark.read.table("{primary.name}")')
            parents = []
            for child, cc, parent, pc in _foreign_keys(model):
                if child is e and parent is not primary and parent.name not in [p[0] for p in parents]:
                    parents.append((parent.name, cc, pc))
            for other in ups[1:]:
                out.append(f'    # TODO join spark.read.table("{other.name}") — also feeds this table')
            for pname, cc, pc in parents:
                out.append(f'    # TODO look up {pname} on {", ".join(cc)} → {", ".join(pc)} if this table needs its attributes')
            if e.columns:
                exprs, todo = _projection(e, primary)
                if todo:
                    out.append(f'    # TODO derive (not in {primary.name}): {", ".join(todo)}')
                out.append("    return df.select(")
                out.extend(f"        {x}," for x in exprs)
                out.append("    )")
            else:
                out.append("    return df")
        elif is_date_dim:
            cols = {c.name.lower(): c for c in e.columns}
            out.append('    days = spark.sql("SELECT explode(sequence(DATE\'2015-01-01\', DATE\'2035-12-31\', INTERVAL 1 DAY)) AS d")')
            gen = {"date": 'F.col("d")', "date_key": 'F.date_format("d", "yyyyMMdd").cast("int")',
                   "year": 'F.year("d")', "quarter": 'F.quarter("d")', "month": 'F.month("d")',
                   "month_name": 'F.date_format("d", "MMMM")', "day": 'F.dayofmonth("d")',
                   "day_of_week": 'F.dayofweek("d")', "week_of_year": 'F.weekofyear("d")',
                   "is_weekend": 'F.dayofweek("d").isin(6, 7)'}
            exprs = []
            for name, c in cols.items():
                expr = gen.get(name) or ('F.col("d")' if name in ("full_date", "calendar_date", "date_value") else None)
                exprs.append(f'{expr}.cast("{_spark_type(c.type)}").alias("{c.name}")' if expr
                             else f'F.lit(None).cast("{_spark_type(c.type)}").alias("{c.name}")')
            out.append("    return days.select(")
            out.extend(f"        {x}," for x in (exprs or ['F.col("d").alias("date")']))
            out.append("    )")
        else:
            out.append(f"    # TODO no upstream in the model — point this at its source; until then it is an")
            out.append(f"    # empty table with the modeled schema so the pipeline still runs.")
            out.append(f'    return spark.createDataFrame([], "{_ddl_schema(e)}")')
        out.append("")

    return "\n".join(out).rstrip() + "\n"
