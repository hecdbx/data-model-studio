# Spark Declarative Pipelines (Lakeflow) authoring

Generate the **modern** Spark Declarative Pipeline API. Legacy `dlt` still works,
but prefer `dp`.

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
```

## Building blocks

- **Materialized view / batch table**: `@dp.table` (or `@dp.materialized_view`) on a
  function that returns a batch DataFrame.
- **Streaming table**: return `spark.readStream…` from a `@dp.table` function, or
  declare `dp.create_streaming_table("name")` and add `@dp.append_flow`.
- **View (not persisted)**: `@dp.view`.
- **Read upstream pipeline datasets**: `spark.read.table("name")` (batch) or
  `spark.readStream.table("name")` (streaming). There is **no** `dp.read` /
  `dp.read_stream` in `pyspark.pipelines` (those were legacy `dlt.read*`).
- **Ingest raw files** with Auto Loader:
  ```python
  spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv").option("header", "true")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(path)
  ```
- **Configuration**: read pipeline settings with `spark.conf.get("key", "default")`.

## Data quality expectations

```python
@dp.table(name="silver_orders", comment="Validated orders")
@dp.expect_or_drop("valid_id", "order_id IS NOT NULL")
@dp.expect("fresh", "order_ts > '2000-01-01'")
def silver_orders():
    return spark.readStream.table("bronze_orders").where("amount >= 0")
```

- `@dp.expect(name, cond)` — warn & keep.
- `@dp.expect_or_drop(name, cond)` — drop violating rows.
- `@dp.expect_or_fail(name, cond)` — fail the update.
- `@dp.expect_all({...})` — a dict of expectations.

## Medallion in a pipeline

- Bronze: Auto Loader streaming tables reading the source volume; add
  `_ingested_at = F.current_timestamp()`.
- Silver: `spark.readStream.table("bronze_…")` (or `spark.read.table` for a
  materialized view), cast/clean/dedupe, expectations on keys.
- Gold: `@dp.materialized_view` aggregations / dimensional joins over silver.

## When the user edits a model

Regenerate only what changed where possible: add a new `@dp.table` function for a
new entity, add expectations for new NOT-NULL keys, and leave existing definitions
intact unless their columns changed. Keep the `spark.conf.get` config block stable.
