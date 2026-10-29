"""Unit tests for pipeline parsing + FK inference (the core Feature-2 logic)."""
import os

from backend.pipelines import (
    infer_relationships_from_source,
    parse_pipeline_code,
    overlay_from_pasted_source,
)


def rels(src):
    return {
        (r["from_table"], tuple(r["from_columns"]), r["to_table"], tuple(r["to_columns"]))
        for r in infer_relationships_from_source(src)
    }


STAR_PY = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="bronze_orders")
def bronze_orders():
    return spark.read.format("csv").load(f"{SRC}/orders.csv")

@dp.table(name="silver_orders")
def silver_orders():
    return spark.read.table("bronze_orders").dropDuplicates()

@dp.materialized_view(name="dim_customer")
def dim_customer():
    return spark.read.table("silver_customers")

@dp.materialized_view(name="dim_date")
def dim_date():
    return spark.read.table("silver_dates")

@dp.materialized_view(name="fact_order")
def fact_order():
    so = spark.read.table("silver_orders")
    dc = spark.read.table("dim_customer")
    dd = spark.read.table("dim_date")
    df = so.join(dc.select("customer_id", "customer_sk"), on="customer_id", how="inner")
    df = df.join(
        dd.select(F.col("date_value").alias("_dv"), "date_sk"),
        df["order_date"] == F.col("_dv"),
        how="left",
    ).drop("_dv")
    return df
'''


def test_produced_tables_and_lineage():
    produced, lineage = parse_pipeline_code(STAR_PY)
    for t in ("bronze_orders", "silver_orders", "dim_customer", "dim_date", "fact_order"):
        assert t in produced, t
    assert {"source": "bronze_orders", "target": "silver_orders"} in lineage


def test_on_join_detected():
    assert ("silver_orders", ("customer_id",), "dim_customer", ("customer_id",)) in rels(STAR_PY)


def test_aliased_fcol_join_detected():
    # df["order_date"] == F.col("_dv"), where _dv aliases dim_date.date_value
    assert ("silver_orders", ("order_date",), "dim_date", ("date_value",)) in rels(STAR_PY)


def test_direction_dim_is_parent():
    for ft, _fc, tt, _tc in rels(STAR_PY):
        assert not (ft.startswith("dim_") and not tt.startswith("dim_")), (ft, tt)


BRACKET_PY = '''
@dp.table(name="fact_x")
def fact_x():
    a = spark.read.table("orders")
    b = spark.read.table("customers")
    return a.join(b, a["customer_id"] == b["id"], how="inner")
'''


def test_bracket_equality_join():
    assert ("orders", ("customer_id",), "customers", ("id",)) in rels(BRACKET_PY)


BROADCAST_PY = '''
@dp.table(name="fact_b")
def fact_b():
    s = spark.read.table("silver_sales")
    d = spark.read.table("dim_store")
    return s.join(F.broadcast(d).select("store_id", "store_sk"), on="store_id")
'''


def test_broadcast_join():
    assert ("silver_sales", ("store_id",), "dim_store", ("store_id",)) in rels(BROADCAST_PY)


SQL_ON = '''
CREATE MATERIALIZED VIEW gold_sales AS
SELECT o.id, c.name, d.year
FROM silver_orders o
JOIN silver_customers c ON o.customer_id = c.id
LEFT JOIN dim_date d ON o.order_date = d.date_value
'''


def test_sql_join_on():
    s = rels(SQL_ON)
    assert ("silver_orders", ("customer_id",), "silver_customers", ("id",)) in s
    # order_date is a real FK to dim_date even though it ends in _date
    assert ("silver_orders", ("order_date",), "dim_date", ("date_value",)) in s


SQL_USING = "CREATE VIEW v AS SELECT * FROM orders JOIN customers USING (customer_id)"


def test_sql_using_join():
    assert ("orders", ("customer_id",), "customers", ("customer_id",)) in rels(SQL_USING)


SPARK_SQL_PY = '''
@dp.table(name="gold_j")
def gold_j():
    return spark.sql("""
        SELECT * FROM silver_a a JOIN dim_b b ON a.b_id = b.id
    """)
'''


def test_spark_sql_embedded():
    assert ("silver_a", ("b_id",), "dim_b", ("id",)) in rels(SPARK_SQL_PY)


def test_overlay_from_pasted_source():
    ov = overlay_from_pasted_source(STAR_PY, "star")
    assert ov["pipeline_id"].startswith("pasted:")
    assert ov["name"] == "star"
    assert "fact_order" in ov["tables"]
    assert ov["inferred_relationships"]


FSTRING_CTE_PY = '''
@dp.materialized_view(name="gold_asset_health")
def gold_asset_health():
    return spark.sql(f"""
        SELECT a.asset_id, s.region
        FROM {S}.silver_assets a
        JOIN {S}.silver_sites s USING (site_id)
    """)

@dp.materialized_view(name="gold_fuel_anomaly")
def gold_fuel_anomaly():
    return spark.sql(f"""
        WITH m AS (
          SELECT i.site_id, s.region
          FROM {S}.silver_energy_invoices i
          JOIN {S}.silver_sites s USING (site_id)
        ),
        recent AS (SELECT site_id FROM m GROUP BY site_id)
        SELECT r.site_id
        FROM (SELECT DISTINCT site_id FROM m) r
        LEFT JOIN recent rc USING (site_id)
    """)
'''


def test_fstring_prefix_and_using():
    # {S}.silver_assets JOIN {S}.silver_sites USING(site_id)
    s = rels(FSTRING_CTE_PY)
    assert ("silver_assets", ("site_id",), "silver_sites", ("site_id",)) in s
    assert ("silver_energy_invoices", ("site_id",), "silver_sites", ("site_id",)) in s


def test_cte_names_never_emitted():
    # m / recent / rc are CTE names/aliases, never real tables
    for ft, _fc, tt, _tc in rels(FSTRING_CTE_PY):
        assert ft not in ("m", "recent", "rc", "r") and tt not in ("m", "recent", "rc", "r")


def test_sql_lineage_from_fstring():
    _produced, lineage = parse_pipeline_code(FSTRING_CTE_PY)
    pairs = {(l["source"], l["target"]) for l in lineage}
    assert ("silver_assets", "gold_asset_health") in pairs
    assert ("silver_sites", "gold_asset_health") in pairs


def test_empty_and_junk_source():
    assert infer_relationships_from_source("") == []
    assert infer_relationships_from_source("print('hello')  # no joins") == []
    assert parse_pipeline_code("") == ([], [])


def test_no_self_relationships():
    for ft, _fc, tt, _tc in rels(STAR_PY):
        assert ft != tt


def test_real_mea_pipeline_if_present():
    """When the exported sample is available, the previously-missed fact→dim
    aliased joins must now be captured with correct direction."""
    p = "/tmp/mea_pipe.py"
    if not os.path.exists(p):
        return
    s = rels(open(p).read())
    assert ("silver_opportunities", ("stage_name",), "dim_opportunity_stage", ("stage_name",)) in s
    assert ("silver_opportunities", ("opp_type",), "dim_opportunity_type", ("opp_type_name",)) in s
    for ft, _fc, tt, _tc in s:
        assert not (ft.startswith("dim_") and not tt.startswith("dim_")), (ft, tt)
    assert len(s) >= 8
