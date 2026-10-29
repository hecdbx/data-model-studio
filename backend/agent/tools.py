"""Agent tools as OpenAI function specs + a dispatch table.

We use the OpenAI-compatible chat client (via databricks-sdk) rather than
langchain, so tools are plain JSON schemas paired with Python callables that
take the parsed-argument dict and return a compact JSON string.
"""

from __future__ import annotations

import json


def _dumps(obj) -> str:
    return json.dumps(obj, default=str)


def build_tools(model_type: str | None = None):
    """Return (specs, funcs): OpenAI tool specs and {name: callable(args_dict)->str}."""
    from ..config import get_settings
    from ..codegen import generate_ddl, generate_sdp
    from ..model_schema import coerce_model
    from ..volumes import get_volume_snapshot, read_file_sample
    from ..uc_explore import build_model_from_schemas
    from ..pipelines import build_pipeline_overlay

    settings = get_settings()

    def _target(model, catalog: str, schema: str):
        cat = catalog or (model.source.catalog if model.source else None) or settings.target_catalog
        sch = schema or (model.source.schemas[0] if (model.source and model.source.schemas)
                         else settings.target_schema)
        return cat, sch

    def scan_volume(a):
        return _dumps(get_volume_snapshot(a["catalog"], a["schema"], a["volume"]))

    def read_file(a):
        return _dumps(read_file_sample(a["file_path"], max_rows=int(a.get("max_rows", 20))))

    def inspect_schema(a):
        model = build_model_from_schemas(a["catalog"], [a["schema"]])
        compact = {
            "entities": [
                {"name": e.name, "layer": e.layer, "primary_key": e.primary_key,
                 "columns": [{"name": c.name, "type": c.type, "pk": c.is_primary_key,
                              "fk": c.is_foreign_key} for c in e.columns]}
                for e in model.entities
            ],
            "relationships": [
                {"from": r.from_entity, "from_cols": r.from_columns,
                 "to": r.to_entity, "to_cols": r.to_columns} for r in model.relationships
            ],
        }
        return _dumps(compact)

    def read_pipeline(a):
        ov = build_pipeline_overlay(a["pipeline_id"])
        if ov.get("source_code") and len(ov["source_code"]) > 12000:
            ov["source_code"] = ov["source_code"][:12000] + "\n# … truncated …"
        return _dumps(ov)

    def gen_ddl(a):
        model = coerce_model(json.loads(a["model_json"]))
        cat, sch = _target(model, a.get("catalog", ""), a.get("schema", ""))
        return generate_ddl(model, cat, sch)

    def gen_sdp(a):
        from ..main import _sdp_source_volume
        model = coerce_model(json.loads(a["model_json"]))
        cat, sch = _target(model, a.get("catalog", ""), a.get("schema", ""))
        return generate_sdp(model, cat, sch, _sdp_source_volume(model, a.get("source_volume")))

    def _spec(name, desc, props, required):
        return {"type": "function", "function": {
            "name": name, "description": desc,
            "parameters": {"type": "object", "properties": props, "required": required},
        }}

    S = lambda d: {"type": "string", "description": d}  # noqa: E731

    specs = [
        _spec("scan_volume",
              "Scan a Unity Catalog volume: list files and read each file's schema + sample rows. Use before designing a model from files.",
              {"catalog": S("Catalog name"), "schema": S("Schema containing the volume"), "volume": S("Volume name")},
              ["catalog", "schema", "volume"]),
        _spec("read_file_sample",
              "Read schema and sample rows from one data file in a volume.",
              {"file_path": S("/Volumes/<catalog>/<schema>/<volume>/file.ext"),
               "max_rows": {"type": "integer", "description": "Sample rows (default 20)"}},
              ["file_path"]),
        _spec("inspect_uc_schema",
              "Introspect an existing Unity Catalog schema: tables, columns, primary keys and foreign-key relationships. Use to ground edits on real UC metadata.",
              {"catalog": S("Catalog name"), "schema": S("Schema to introspect")},
              ["catalog", "schema"]),
        _spec("read_pipeline_source",
              "Fetch a Lakeflow pipeline's source code, the tables it builds, and its internal lineage.",
              {"pipeline_id": S("Lakeflow pipeline id")},
              ["pipeline_id"]),
        _spec("generate_sql_ddl",
              "Deterministically generate full Unity Catalog DDL (CREATE TABLE + PK/FK) for a model. Prefer this over hand-writing full DDL.",
              {"model_json": S("The data model as a JSON string (canvas schema)"),
               "catalog": S("Target catalog (optional)"), "schema": S("Target schema (optional)")},
              ["model_json"]),
        _spec("generate_sdp_pipeline",
              "Deterministically generate a Spark Declarative Pipeline (pipeline.py) scaffold for a model. Prefer this over hand-writing a full pipeline.",
              {"model_json": S("The data model as a JSON string (canvas schema)"),
               "catalog": S("Target catalog (optional)"), "schema": S("Target schema (optional)")},
              ["model_json"]),
    ]

    funcs = {
        "scan_volume": scan_volume,
        "read_file_sample": read_file,
        "inspect_uc_schema": inspect_schema,
        "read_pipeline_source": read_pipeline,
        "generate_sql_ddl": gen_ddl,
        "generate_sdp_pipeline": gen_sdp,
    }
    return specs, funcs
