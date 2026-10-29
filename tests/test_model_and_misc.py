"""Unit tests for model schema, identifier validation, and layer inference."""
import pytest

from backend.model_schema import coerce_model, DataModel
from backend.databricks_clients import valid_ident, quote_ident
from backend.uc_explore import infer_layer, _in_list


def test_coerce_empty():
    m = coerce_model({})
    assert isinstance(m, DataModel)
    assert m.entities == [] and m.relationships == []


def test_coerce_schema_alias():
    # incoming "schema" maps to schema_name; extra keys are allowed
    m = coerce_model({"entities": [{"id": "x", "name": "t", "schema": "s1", "hint": "z",
                                    "columns": [{"name": "a"}]}]})
    e = m.entities[0]
    assert e.schema_name == "s1"
    assert e.columns[0].type == "STRING"  # default


def test_coerce_entity_by_id_and_name():
    m = coerce_model({"entities": [{"id": "s.t", "name": "t", "columns": []}]})
    assert m.entity_by_id("s.t").name == "t"
    assert m.entity_by_id("t").id == "s.t"
    assert m.entity_by_id("missing") is None


def test_valid_ident():
    assert valid_ident("houssem_demo_catalog")
    assert valid_ident("Table123")
    assert not valid_ident("bad-name")
    assert not valid_ident("has space")
    assert not valid_ident("")
    assert not valid_ident("a;b")


def test_quote_ident():
    assert quote_ident("mytable") == "`mytable`"
    with pytest.raises(ValueError):
        quote_ident("drop table;")


@pytest.mark.parametrize("schema,table,expected", [
    ("s", "fact_sales", "fact"),
    ("s", "dim_customer", "dimension"),
    ("s", "bridge_a_b", "bridge"),
    ("s", "hub_x", "hub"),
    ("s", "sat_x_y", "satellite"),
    ("s", "bronze_raw", "bronze"),
    ("s", "silver_clean", "silver"),
    ("s", "gold_kpi", "gold"),
    ("sales_gold", "anything", "gold"),
    ("s", "plain_table", "table"),
])
def test_infer_layer(schema, table, expected):
    assert infer_layer(schema, table) == expected


def test_in_list_builds_params():
    clause, params = _in_list(["a", "b"])
    assert clause == "(:s0, :s1)"
    assert params == {"s0": "a", "s1": "b"}


def test_in_list_rejects_all_invalid():
    with pytest.raises(ValueError):
        _in_list(["bad-name", "also bad"])


def test_coerce_canvas_only_source_kinds():
    # the in-app sample model is tagged "sample"; a kind only the canvas knows is a draft
    assert coerce_model({"source": {"kind": "sample"}}).source.kind == "sample"
    assert coerce_model({"source": {"kind": "something_new"}}).source.kind == "blank"


def test_sample_model_generates_code():
    from backend.codegen import generate_ddl, generate_sdp
    col = lambda n, t="BIGINT", **kw: {"name": n, "type": t, **kw}
    m = coerce_model({
        "name": "commerce_model", "source": {"kind": "sample"},
        "entities": [
            {"id": "customers", "name": "customers", "schema": "commerce", "primary_key": ["customer_id"],
             "columns": [col("customer_id", is_primary_key=True, nullable=False), col("email", "STRING")]},
            {"id": "orders", "name": "orders", "schema": "commerce", "primary_key": ["order_id"],
             "columns": [col("order_id", is_primary_key=True, nullable=False), col("customer_id", is_foreign_key=True)]},
        ],
        "relationships": [{"id": "r1", "from_entity": "orders", "from_columns": ["customer_id"],
                           "to_entity": "customers", "to_columns": ["customer_id"]}],
    })
    ddl = generate_ddl(m, "cat", "sch")
    assert "CREATE SCHEMA IF NOT EXISTS `cat`.`commerce`" in ddl
    assert "PRIMARY KEY (`order_id`)" in ddl and "FOREIGN KEY (`customer_id`)" in ddl
    compile(generate_sdp(m, "cat", "sch"), "pipeline.py", "exec")
