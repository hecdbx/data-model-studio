"""Relationship cardinality: a FK that is the child's whole primary key is one-to-one,
anything else many-to-one — on every path that creates relationships."""
from backend import pipelines as pl
from backend.agent.agent import _apply_key_diff, expand_compact_model
from backend.model_schema import infer_cardinality


def _ent(schema, name, cols, pk=()):
    return {"id": f"{schema}.{name}", "name": name, "schema": schema, "primary_key": list(pk),
            "columns": [{"name": c, "is_primary_key": c in pk} for c in cols]}


def test_infer_cardinality():
    assert infer_cardinality(["site_id"], ["site_id"]) == "one-to-one"
    assert infer_cardinality(["tower_id", "slot_no"], ["slot_no", "TOWER_ID"]) == "one-to-one"
    assert infer_cardinality(["tower_id", "slot_no"], ["tower_id"]) == "many-to-one"
    assert infer_cardinality(["tower_id"], ["site_id"]) == "many-to-one"
    assert infer_cardinality([], ["site_id"]) == "many-to-one"
    assert infer_cardinality(["id"], []) == "many-to-one"


def test_uc_relationships_carry_cardinality(monkeypatch):
    from backend import uc_explore as uc

    def kcu(name, table, col, piu=None):
        return {"constraint_schema": "ops", "constraint_name": name, "table_schema": "ops", "table_name": table,
                "column_name": col, "ordinal_position": 1, "position_in_unique_constraint": piu}

    rows = {
        "information_schema.tables": [{"table_schema": "ops", "table_name": t, "table_type": "MANAGED", "comment": ""}
                                      for t in ("sites", "site_power_profiles", "towers")],
        "information_schema.columns": [
            {"table_schema": "ops", "table_name": t, "column_name": c, "full_data_type": "string",
             "is_nullable": "NO", "comment": "", "ordinal_position": i}
            for t, cols in (("sites", ["site_id"]), ("site_power_profiles", ["site_id", "power_source"]),
                            ("towers", ["tower_id", "site_id"])) for i, c in enumerate(cols, 1)],
        "information_schema.table_constraints": [
            {"constraint_schema": "ops", "constraint_name": n, "table_schema": "ops", "table_name": t, "constraint_type": k}
            for n, t, k in (("pk_sites", "sites", "PRIMARY KEY"), ("pk_spp", "site_power_profiles", "PRIMARY KEY"),
                            ("fk_spp_site", "site_power_profiles", "FOREIGN KEY"), ("pk_towers", "towers", "PRIMARY KEY"),
                            ("fk_towers_site", "towers", "FOREIGN KEY"))],
        "information_schema.key_column_usage": [
            kcu("pk_sites", "sites", "site_id"), kcu("pk_spp", "site_power_profiles", "site_id"),
            kcu("fk_spp_site", "site_power_profiles", "site_id", 1), kcu("pk_towers", "towers", "tower_id"),
            kcu("fk_towers_site", "towers", "site_id", 1)],
        "information_schema.referential_constraints": [
            {"constraint_schema": "ops", "constraint_name": fk, "unique_constraint_schema": "ops",
             "unique_constraint_name": "pk_sites"} for fk in ("fk_spp_site", "fk_towers_site")],
    }

    def fake_sql(sql, parameters=None):
        return next(v for k, v in rows.items() if f"{k} " in sql)

    monkeypatch.setattr(uc, "run_sql_dicts", fake_sql)
    m = uc.build_model_from_schemas("cat", ["ops"])
    assert {(r.from_entity, r.to_entity): r.type for r in m.relationships} == {
        ("ops.site_power_profiles", "ops.sites"): "one-to-one",
        ("ops.towers", "ops.sites"): "many-to-one",
    }


def test_pipeline_join_on_shared_key_is_one_to_one():
    src = '''
from pyspark import pipelines as dp

@dp.materialized_view(name="site_power_report")
def site_power_report():
    profiles = spark.read.table("site_power_profiles")
    towers = spark.read.table("towers")
    sites = spark.read.table("sites")
    return profiles.join(sites, "site_id").join(towers.join(sites, "site_id"), "site_id")
'''
    ents = [_ent("ops", "sites", ["site_id", "site_name"], pk=["site_id"]),
            _ent("ops", "site_power_profiles", ["site_id", "power_source"], pk=["site_id"]),
            _ent("ops", "towers", ["tower_id", "site_id"], pk=["tower_id"])]
    rels = pl.resolve_relationships(pl.overlay_from_pasted_source(src, "power"), ents)
    types = {(r["from_entity"], r["to_entity"]): r["type"] for r in rels}
    assert types[("ops.site_power_profiles", "ops.sites")] == "one-to-one"
    assert types[("ops.towers", "ops.sites")] == "many-to-one"


def test_agent_paths_infer_cardinality():
    m = expand_compact_model({"entities": [
        {"name": "sites", "pk": ["site_id"], "columns": ["site_id STRING"]},
        {"name": "site_power_profiles", "pk": ["site_id"], "columns": ["site_id STRING", "power_source STRING"]},
        {"name": "towers", "pk": ["tower_id"], "columns": ["tower_id STRING", "site_id STRING"]},
    ], "relationships": [["site_power_profiles", ["site_id"], "sites", ["site_id"]],
                         ["towers", ["site_id"], "sites", ["site_id"]]]})
    assert [r["type"] for r in m["relationships"]] == ["one-to-one", "many-to-one"]

    base = {"entities": [
        {"id": "s", "name": "sites", "columns": [{"name": "site_id"}]},
        {"id": "p", "name": "site_power_profiles", "columns": [{"name": "site_id"}]},
        {"id": "t", "name": "towers", "columns": [{"name": "tower_id"}, {"name": "site_id"}]},
    ], "relationships": []}
    out = _apply_key_diff(base, {
        "primary_keys": {"s": ["site_id"], "p": ["site_id"], "t": ["tower_id"]},
        "foreign_keys": [
            {"from_entity": "p", "from_columns": ["site_id"], "to_entity": "s", "to_columns": ["site_id"]},
            {"from_entity": "t", "from_columns": ["site_id"], "to_entity": "s", "to_columns": ["site_id"]},
            # an explicit cardinality from the agent is kept as-is
            {"from_entity": "t", "from_columns": ["tower_id"], "to_entity": "p", "to_columns": ["site_id"],
             "type": "many-to-many"},
        ]})
    assert [r["type"] for r in out["relationships"]] == ["one-to-one", "many-to-one", "many-to-many"]
