"""Regression tests for the bugs found while testing against the Tawal synthetic
dataset (demo_data/tawal): real-world pipeline patterns, FK resolution onto the
canvas, composite-key codegen, internal-table filtering, volume snapshots, and
the agent's canvas context."""
import glob
import json
import os
from types import SimpleNamespace

import pytest

from backend import pipelines as pl
from backend.agent.agent import _compact_model, _context_message, extract_model_from_text
from backend.codegen import generate_ddl, generate_migration
from backend.model_schema import coerce_model
from backend.uc_explore import _INTERNAL_RE

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPES = os.path.join(ROOT, "demo_data", "tawal", "pipelines")


def _files(layer):
    paths = sorted(glob.glob(os.path.join(PIPES, layer, "transformations", "*")))
    return [(p, open(p, encoding="utf-8").read()) for p in paths]


def _overlay(layer, target):
    files = _files(layer)
    return pl._assemble_overlay(layer, layer, "hec_demo_catalog", target, files, [p for p, _ in files])


def _ent(schema, name, cols, pk=()):
    return {"id": f"{schema}.{name}", "name": name, "schema": schema, "primary_key": list(pk),
            "columns": [{"name": c, "is_primary_key": c in pk} for c in cols]}


def _resolved(ov, ents):
    return {(r["from_entity"], tuple(r["from_columns"]), r["to_entity"], tuple(r["to_columns"]))
            for r in pl.resolve_relationships(ov, ents)}


# ── parsing real pipeline shapes ─────────────────────────────────────────────
def test_metadata_driven_loop_expands_every_table():
    ov = _overlay("bronze", "tawal_bronze")
    assert len(ov["tables"]) == 18
    assert "bronze_sites" in ov["tables"] and "bronze_energy_readings" in ov["tables"]
    assert not any(t in ("make_bronze", "bronze", "_bronze") or "{" in t for t in ov["tables"])


def test_dynamic_name_never_emits_function_name():
    src = '''
from pyspark import pipelines as dp
def factory(x):
    @dp.table(name=f"t_{x}")
    def inner():
        return spark.read.table("src")
'''  # factory is never called with a constant → name unknown → nothing emitted
    produced, _ = pl.parse_pipeline_code(src)
    assert produced == []


def test_sql_files_get_lineage_and_stream_is_not_a_table():
    ov = _overlay("silver", "tawal_silver")
    pairs = {(l["source"], l["target"]) for l in ov["lineage"]}
    assert ("bronze_sites", "silver_sites") in pairs            # SQL MV
    assert ("bronze_cities", "silver_sites") in pairs           # SQL join
    assert ("bronze_lease_invoices", "silver_lease_invoices") in pairs  # FROM STREAM(...)
    assert not any(s.lower() == "stream" for s, _ in pairs)


def test_mixed_python_and_sql_files_both_infer_joins():
    rels = _overlay("silver", "tawal_silver")["inferred_relationships"]
    srcs = {r["source"] for r in rels}
    assert {"pipeline-sql", "pipeline-join"} <= srcs


def test_positional_on_chained_and_fstring_tables():
    src = '''
from pyspark import pipelines as dp
CATALOG = spark.conf.get("tawal.catalog", "cat")
SILVER = f"{CATALOG}.silver"

@dp.materialized_view(name="dim_site")
def dim_site():
    sites = spark.read.table(f"{SILVER}.silver_sites")
    cities = spark.read.table(f"{SILVER}.silver_cities").select("city_id", "city_name")
    regions = spark.read.table(f"{SILVER}.silver_regions")
    return (
        sites.join(cities, "city_id", "left")
        .join(regions, ["region_id"], "left")
    )
'''
    rels = {(r["from_table"], tuple(r["from_columns"]), r["to_table"])
            for r in pl.infer_relationships_from_source(src)}
    assert ("silver_sites", ("city_id",), "silver_cities") in rels
    assert ("silver_sites", ("region_id",), "silver_regions") in rels  # 2nd link of the chain


def test_assignment_alias_and_fcol_condition():
    src = '''
@dp.materialized_view(name="dim_field_team")
def dim_field_team():
    teams = spark.read.table("silver_field_teams")
    vendors = spark.read.table("dim_vendor").select(F.col("vendor_id").alias("_contractor_id"))
    return teams.join(vendors, teams["contractor_id"] == F.col("_contractor_id"), "left")

@dp.materialized_view(name="fact_x")
def fact_x():
    inv = spark.read.table("silver_invoices")
    dd = spark.read.table("dim_date")
    return inv.join(dd, F.col("billing_month") == dd.date, "left")
'''
    rels = {(r["from_table"], tuple(r["from_columns"]), r["to_table"], tuple(r["to_columns"]))
            for r in pl.infer_relationships_from_source(src)}
    assert ("silver_field_teams", ("contractor_id",), "dim_vendor", ("vendor_id",)) in rels
    assert ("silver_invoices", ("billing_month",), "dim_date", ("date",)) in rels


def test_sql_flow_and_private_mv():
    src = """
CREATE OR REFRESH STREAMING TABLE silver_events;
CREATE FLOW ingest AS INSERT INTO silver_events BY NAME SELECT * FROM STREAM(bronze_events);
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW tmp_x AS SELECT * FROM silver_events;
"""
    produced, lineage = pl.parse_pipeline_code(src)
    assert "silver_events" in produced and "tmp_x" in produced
    pairs = {(l["source"], l["target"]) for l in lineage}
    assert ("bronze_events", "silver_events") in pairs and ("silver_events", "tmp_x") in pairs


def test_tvf_in_from_is_not_a_table():
    src = "CREATE MATERIALIZED VIEW r AS SELECT * FROM read_files('/Volumes/a/b/c', format => 'csv')"
    _, lineage = pl.parse_pipeline_code(src)
    assert lineage == []


# ── resolving onto the canvas ────────────────────────────────────────────────
def test_silver_fks_carried_along_lineage():
    ov = _overlay("silver", "tawal_silver")
    ents = [
        _ent("tawal_silver", "silver_sites", ["site_id", "city_id", "region_id"]),
        _ent("tawal_silver", "silver_cities", ["city_id", "region_id", "city_name"]),
        _ent("tawal_silver", "silver_regions", ["region_id", "region_name"]),
        _ent("tawal_silver", "silver_work_orders", ["work_order_id", "alarm_id"]),
        _ent("tawal_silver", "silver_alarms", ["alarm_id", "site_id"]),
    ]
    got = _resolved(ov, ents)
    assert ("tawal_silver.silver_sites", ("city_id",), "tawal_silver.silver_cities", ("city_id",)) in got
    assert ("tawal_silver.silver_cities", ("region_id",), "tawal_silver.silver_regions", ("region_id",)) in got
    assert ("tawal_silver.silver_work_orders", ("alarm_id",), "tawal_silver.silver_alarms", ("alarm_id",)) in got


def test_gold_facts_link_dims_by_carried_surrogate_key():
    ov = _overlay("gold", "tawal_gold")
    ents = [
        _ent("tawal_gold", "dim_site", ["site_key", "site_id", "region_id"]),
        _ent("tawal_gold", "dim_region", ["region_key", "region_id"]),
        _ent("tawal_gold", "dim_tenant", ["tenant_key", "tenant_id"]),
        _ent("tawal_gold", "dim_date", ["date_key", "date"]),
        _ent("tawal_gold", "fact_lease_revenue", ["invoice_id", "lease_id", "site_key", "tenant_key", "date_key"]),
        _ent("tawal_silver", "silver_leases", ["lease_id", "site_id"]),
    ]
    got = _resolved(ov, ents)
    for parent, key in (("dim_site", "site_key"), ("dim_tenant", "tenant_key"), ("dim_date", "date_key")):
        assert ("tawal_gold.fact_lease_revenue", (key,), f"tawal_gold.{parent}", (key,)) in got
    assert ("tawal_gold.dim_site", ("region_id",), "tawal_gold.dim_region", ("region_id",)) in got
    # an output is never linked back to the input it was built from
    assert not any(c == "tawal_gold.fact_lease_revenue" and p == "tawal_silver.silver_leases" for c, _, p, _ in got)
    assert not any(c == "tawal_silver.silver_leases" and p.startswith("tawal_gold.") for c, _, p, _ in got)


def test_shared_fk_join_is_not_a_relationship():
    # energy ⋈ bills on site_id: both reference sites; site_id isn't a key of bills
    ov = _overlay("gold", "tawal_gold")
    ents = [_ent("tawal_silver", "silver_energy_readings", ["reading_id", "site_id"]),
            _ent("tawal_silver", "silver_utility_bills", ["bill_id", "site_id"])]
    assert _resolved(ov, ents) == set()


def test_known_pk_is_authoritative_and_partial_composite_rejected():
    ov = {"tables": [], "lineage": [], "primary_sources": {}, "inferred_relationships": [
        {"from_table": "leases", "from_columns": ["tower_id"], "to_table": "tower_slots",
         "to_columns": ["tower_id"], "from_candidates": ["leases"], "context_table": None}]}
    ents = [_ent("o", "leases", ["lease_id", "tower_id", "slot_no"], pk=("lease_id",)),
            _ent("o", "tower_slots", ["tower_id", "slot_no"], pk=("tower_id", "slot_no"))]
    assert _resolved(ov, ents) == set()


def test_role_playing_joins_keep_both_links():
    src = """
CREATE MATERIALIZED VIEW link_report AS
SELECT l.link_id FROM fiber_links l
JOIN sites a ON l.a_site_id = a.site_id
JOIN sites z ON l.z_site_id = z.site_id
"""
    ov = pl.overlay_from_pasted_source(src)
    ents = [_ent("o", "fiber_links", ["link_id", "a_site_id", "z_site_id"]), _ent("o", "sites", ["site_id"])]
    got = _resolved(ov, ents)
    assert ("o.fiber_links", ("a_site_id",), "o.sites", ("site_id",)) in got
    assert ("o.fiber_links", ("z_site_id",), "o.sites", ("site_id",)) in got


# ── glob libraries ───────────────────────────────────────────────────────────
def test_glob_library_lists_source_files():
    from databricks.sdk.service.workspace import ObjectType

    root = "/Workspace/Users/u/.bundle/p/files/pipelines/silver"
    objs = [SimpleNamespace(path=f"{root}/transformations/a.sql", object_type=ObjectType.FILE),
            SimpleNamespace(path=f"{root}/transformations/sub/b.py", object_type=ObjectType.FILE),
            SimpleNamespace(path=f"{root}/transformations/readme.md", object_type=ObjectType.FILE),
            SimpleNamespace(path=f"{root}/transformations/nb", object_type=ObjectType.NOTEBOOK)]

    class W:
        class workspace:  # noqa: N801
            @staticmethod
            def list(path, recursive=False):
                assert path == f"{root}/transformations" and recursive
                return objs

    got = pl._glob_files(f"{root}/transformations/**", export_client=W)
    assert got == sorted([f"{root}/transformations/a.sql", f"{root}/transformations/sub/b.py",
                          f"{root}/transformations/nb"])


# ── codegen ──────────────────────────────────────────────────────────────────
def _slots_model(with_rel=True):
    return coerce_model({
        "name": "t",
        "entities": [
            {"id": "o.tower_slots", "name": "tower_slots", "schema": "o",
             "columns": [{"name": "tower_id", "is_primary_key": True}, {"name": "slot_no", "type": "INT", "is_primary_key": True}],
             "primary_key": ["tower_id", "slot_no"]},
            {"id": "o.leases", "name": "leases", "schema": "o",
             "columns": [{"name": "lease_id", "is_primary_key": True},
                         {"name": "tower_id", "is_foreign_key": True, "references": {"entity": "o.tower_slots", "column": "tower_id"}},
                         {"name": "slot_no", "type": "INT", "is_foreign_key": True, "references": {"entity": "o.tower_slots", "column": "slot_no"}}],
             "primary_key": ["lease_id"]},
        ],
        "relationships": ([{"id": "r", "from_entity": "o.leases", "from_columns": ["tower_id", "slot_no"],
                            "to_entity": "o.tower_slots", "to_columns": ["tower_id", "slot_no"]}] if with_rel else []),
    })


def test_composite_fk_emitted_once():
    ddl = generate_ddl(_slots_model(), "c", "o")
    assert ddl.count("FOREIGN KEY") == 1
    assert "FOREIGN KEY (`tower_id`, `slot_no`) REFERENCES `c`.`o`.`tower_slots` (`tower_id`, `slot_no`)" in ddl


def test_column_refs_into_composite_key_are_grouped():
    ddl = generate_ddl(_slots_model(with_rel=False), "c", "o")
    assert ddl.count("FOREIGN KEY") == 1 and "FOREIGN KEY (`tower_id`, `slot_no`)" in ddl


def test_fk_to_non_pk_is_commented_not_emitted():
    m = coerce_model({"entities": [
        {"id": "a", "name": "a", "columns": [{"name": "id", "is_primary_key": True}, {"name": "b_code"}], "primary_key": ["id"]},
        {"id": "b", "name": "b", "columns": [{"name": "id", "is_primary_key": True}, {"name": "code"}], "primary_key": ["id"]},
    ], "relationships": [{"id": "r", "from_entity": "a", "from_columns": ["b_code"], "to_entity": "b", "to_columns": ["code"]}]})
    ddl = generate_ddl(m, "c", "s")
    assert "FOREIGN KEY" not in ddl and "-- skipped FK a(b_code) -> b" in ddl
    mig = generate_migration(coerce_model({"entities": m.model_dump(by_alias=True)["entities"]}), m, "c", "s")
    assert "FOREIGN KEY" not in mig and "SKIPPED FK" in mig


# ── UC + volumes ─────────────────────────────────────────────────────────────
@pytest.mark.parametrize("name,hidden", [
    ("event_log_f8b64359_904d_4be6_a900_e6e7928f3223", True),
    ("__materialization_mat_x_bronze_sites_1", True),
    ("event_log_summary", False),
    ("silver_sites", False),
])
def test_pipeline_internal_tables_hidden(name, hidden):
    assert bool(_INTERNAL_RE.match(name)) is hidden


def test_volume_snapshot_samples_one_file_per_folder(monkeypatch):
    from backend import volumes as vol

    tree = {"/Volumes/c/s/v": [("sites", True), ("energy", True), ("top.csv", False), ("notes.md", False)],
            "/Volumes/c/s/v/sites": [("sites.csv", False)],
            "/Volumes/c/s/v/energy": [("e_07.csv", False), ("e_08.csv", False), ("e_09.csv", False)]}

    def fake_list(path):
        return [{"name": n, "path": f"{path}/{n}", "is_directory": d, "file_size": 1} for n, d in tree.get(path, [])]

    sampled = []

    def fake_sample(path, max_rows=30):
        sampled.append(path)
        return {"file_path": path, "columns": ["a"], "sample_rows": []}

    monkeypatch.setattr(vol, "list_volume_files", fake_list)
    monkeypatch.setattr(vol, "read_file_sample", fake_sample)
    snap = vol.get_volume_snapshot("c", "s", "v")
    assert snap["total_data_sources"] == 3
    assert sorted(sampled) == sorted(["/Volumes/c/s/v/sites/sites.csv", "/Volumes/c/s/v/energy/e_07.csv",
                                      "/Volumes/c/s/v/top.csv"])
    energy = next(d for d in snap["data_sources"] if d["dataset"] == "energy")
    assert energy["files_in_dataset"] == 3


# ── agent canvas context ─────────────────────────────────────────────────────
def _big_model(n=60, cols=12):
    return {"name": "big", "entities": [
        {"id": f"s.t{i}", "name": f"t{i}", "schema": "s", "description": "d" * 300,
         "columns": [{"name": f"c{j}", "type": "STRING", "description": "x" * 200, "is_primary_key": j == 0}
                     for j in range(cols)]} for i in range(n)],
        "relationships": [{"id": f"r{i}", "from_entity": f"s.t{i}", "from_columns": ["c1"],
                           "to_entity": "s.t0", "to_columns": ["c0"]} for i in range(1, n)]}


def test_compact_model_is_valid_json_with_every_table():
    s = _compact_model(_big_model(), budget=60000)
    obj = json.loads(s)  # never cut mid-object
    assert len(obj["entities"]) == 60 and len(s) <= 60000


def test_compact_model_degrades_instead_of_truncating():
    s = _compact_model(_big_model(n=400, cols=30), budget=20000)
    obj = json.loads(s)
    assert len(obj["entities"]) == 400


def test_context_asks_for_a_patch():
    ctx = _context_message(_big_model(n=3), None)
    assert "removed_entities" in ctx and "3 tables" in ctx


def test_patch_is_extracted_as_model():
    txt = 'Done.\n```json\n{"removed_entities": ["s.t1"]}\n```'
    assert extract_model_from_text(txt) == {"removed_entities": ["s.t1"]}


# ── compact generation format ────────────────────────────────────────────────
def test_expand_compact_model():
    from backend.agent.agent import expand_compact_model

    m = expand_compact_model({
        "model_type": "medallion", "name": "tawal",
        "entities": [
            {"name": "silver_sites", "layer": "silver", "pk": ["site_id"],
             "columns": ["site_id STRING", "city_id INT", "monthly_fee_sar DECIMAL(12, 2)"]},
            {"name": "silver_cities", "schema": "silver", "layer": "silver", "pk": "city_id",
             "columns": [["city_id", "int"], {"name": "city_name", "type": "STRING"}]},
        ],
        "relationships": [["silver_sites", ["city_id"], "silver_cities", ["city_id"]],
                          ["silver_sites", ["nope"], "missing_table", ["x"]]],
    })
    sites, cities = m["entities"]
    assert sites["id"] == "silver_sites" and cities["id"] == "silver.silver_cities"
    assert [c["type"] for c in sites["columns"]] == ["STRING", "INT", "DECIMAL(12, 2)"]
    assert sites["columns"][0]["is_primary_key"] and not sites["columns"][0]["nullable"]
    assert cities["primary_key"] == ["city_id"] and cities["columns"][1]["name"] == "city_name"
    assert len(m["relationships"]) == 1
    r = m["relationships"][0]
    assert (r["from_entity"], r["to_entity"]) == ("silver_sites", "silver.silver_cities")
    fk = sites["columns"][1]
    assert fk["is_foreign_key"] and fk["references"] == {"entity": "silver.silver_cities", "column": "city_id"}
    # the expanded model is valid for codegen
    ddl = generate_ddl(coerce_model(m), "c", "s")
    assert "FOREIGN KEY (`city_id`) REFERENCES `c`.`silver`.`silver_cities` (`city_id`)" in ddl


def test_expand_passes_canvas_schema_through():
    from backend.agent.agent import expand_compact_model

    canvas = {"entities": [{"id": "a", "name": "a", "columns": [{"name": "id", "type": "BIGINT", "is_primary_key": True}],
                            "primary_key": ["id"]}], "relationships": []}
    out = expand_compact_model(canvas)
    assert out["entities"][0]["columns"][0]["is_primary_key"] and out["entities"][0]["id"] == "a"


# ── SDP generator produces a runnable pipeline ───────────────────────────────
def _medallion_model():
    from backend.agent.agent import expand_compact_model
    return coerce_model(expand_compact_model({
        "model_type": "medallion", "name": "tawal",
        "source": {"kind": "volume", "catalog": "c", "schema": "raw", "volume": "landing"},
        "entities": [
            {"name": "bronze_sites", "layer": "bronze", "source": "sites", "format": "csv",
             "columns": ["site_id STRING", "city_id STRING", "_ingested_at TIMESTAMP", "_source_file STRING"]},
            {"name": "bronze_alarms", "layer": "bronze", "source": "alarms", "format": "json",
             "columns": ["alarm_id STRING", "site_id STRING"]},
            {"name": "silver_sites", "layer": "silver", "pk": ["site_id"],
             "columns": ["site_id STRING", "city_id INT", "region_code STRING"]},
            {"name": "silver_cities", "layer": "silver", "pk": ["city_id"], "columns": ["city_id INT"]},
            {"name": "dim_site", "layer": "gold", "pk": ["site_key"],
             "columns": ["site_key BIGINT", "site_id STRING", "city_id INT"]},
            {"name": "dim_date", "layer": "gold", "pk": ["date_key"],
             "columns": ["date_key INT", "date DATE", "year INT", "is_weekend BOOLEAN"]},
        ],
        "relationships": [["silver_sites", ["city_id"], "silver_cities", ["city_id"]]],
    }))


def test_sdp_reads_real_dataset_paths_and_formats():
    sdp = generate_sdp_src = __import__("backend.codegen", fromlist=["generate_sdp"]).generate_sdp(
        _medallion_model(), "c", "s", "/Volumes/c/raw/landing")
    compile(sdp, "pipeline.py", "exec")
    assert 'SOURCE_VOLUME = spark.conf.get("source_volume_path", "/Volumes/c/raw/landing")' in sdp
    assert 'path = f"{SOURCE_VOLUME}/sites"' in sdp and 'path = f"{SOURCE_VOLUME}/alarms"' in sdp
    assert '.option("cloudFiles.format", "json")' in sdp
    assert 'df = df.withColumn("_source_file", F.col("_metadata.file_path"))' in sdp
    assert "dp.read(" not in sdp and "dp.read_stream(" not in sdp


def test_sdp_upstream_is_same_entity_one_layer_down_not_fk_parent():
    from backend.codegen import generate_sdp
    sdp = generate_sdp(_medallion_model(), "c", "s", "/Volumes/c/raw/landing")
    silver = sdp.split("def silver_sites():")[1].split("@dp.")[0]
    assert 'spark.read.table("bronze_sites")' in silver
    assert 'F.col("city_id").cast("int").alias("city_id")' in silver
    assert 'F.lit(None).cast("string").alias("region_code")' in silver and "TODO derive" in silver
    dim = sdp.split("def dim_site():")[1].split("@dp.")[0]
    assert 'spark.read.table("silver_sites")' in dim
    assert 'F.xxhash64(F.col("site_id")).cast("bigint").alias("site_key")' in dim
    cal = sdp.split("def dim_date():")[1]
    assert "sequence(DATE" in cal and 'F.date_format("d", "yyyyMMdd").cast("int")' in cal


def test_sdp_source_volume_defaults_to_seeded_volume():
    from backend import main  # noqa: F401  (import check only; endpoint logic below)
    from backend.main import _sdp_source_volume
    m = _medallion_model()
    assert _sdp_source_volume(m, None) == "/Volumes/c/raw/landing"
    assert _sdp_source_volume(m, "/Volumes/x/y/z") == "/Volumes/x/y/z"
    assert _sdp_source_volume(coerce_model({"entities": []}), None) is None
