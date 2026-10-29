"""Unit tests for deterministic code generation (DDL / SDP / migration)."""
from backend.codegen import generate_ddl, generate_sdp, generate_migration
from backend.model_schema import coerce_model


def _model(extra_col=False):
    orders_cols = [
        {"name": "id", "type": "BIGINT", "is_primary_key": True, "nullable": False},
        {"name": "customer_id", "type": "BIGINT", "is_foreign_key": True,
         "references": {"entity": "s.customers", "column": "id"}},
    ]
    if extra_col:
        orders_cols.append({"name": "total", "type": "DOUBLE"})
    return coerce_model({
        "name": "shop",
        "entities": [
            {"id": "s.customers", "name": "customers", "schema": "s", "layer": "dimension",
             "columns": [
                 {"name": "id", "type": "BIGINT", "is_primary_key": True, "nullable": False},
                 {"name": "email", "type": "STRING"},
             ], "primary_key": ["id"]},
            {"id": "s.orders", "name": "orders", "schema": "s", "layer": "fact",
             "columns": orders_cols, "primary_key": ["id"]},
        ],
        "relationships": [
            {"id": "r1", "from_entity": "s.orders", "from_columns": ["customer_id"],
             "to_entity": "s.customers", "to_columns": ["id"], "type": "many-to-one"},
        ],
    })


def test_ddl_tables_pk_fk():
    ddl = generate_ddl(_model(), "cat", "sch")
    assert "CREATE TABLE IF NOT EXISTS" in ddl
    assert "`customers`" in ddl and "`orders`" in ddl
    assert "PRIMARY KEY" in ddl
    assert "FOREIGN KEY" in ddl and "REFERENCES" in ddl
    # FK references the parent's PK column (entities carry schema "s")
    assert "REFERENCES `cat`.`s`.`customers` (`id`)" in ddl


def test_ddl_dimension_created_before_fact():
    # dimension (layer rank 1) should be created before fact (rank 2)
    ddl = generate_ddl(_model(), "cat", "sch")
    assert ddl.index("`customers`") < ddl.index("CREATE TABLE IF NOT EXISTS `cat`.`s`.`orders`")


def test_sdp_python_scaffold():
    sdp = generate_sdp(_model(), "cat", "sch")
    assert "from pyspark import pipelines as dp" in sdp
    assert "@dp.table(" in sdp or "@dp.materialized_view(" in sdp
    assert "def customers():" in sdp and "def orders():" in sdp
    assert "dp.read(" not in sdp  # not part of the pyspark.pipelines API
    compile(sdp, "pipeline.py", "exec")


def test_migration_additive_add_column():
    mig = generate_migration(_model(), _model(extra_col=True), "cat", "sch")
    assert "ADD COLUMN `total`" in mig
    assert "No additive changes" not in mig


def test_migration_no_changes():
    mig = generate_migration(_model(), _model(), "cat", "sch")
    assert "No additive changes detected." in mig


def test_migration_flags_dropped_column():
    # base has `total`, edited doesn't -> a flagged (non-applied) warning
    mig = generate_migration(_model(extra_col=True), _model(), "cat", "sch")
    assert "DROPPED COLUMN" in mig
