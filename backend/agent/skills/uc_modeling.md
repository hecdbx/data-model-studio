# Unity Catalog modeling reference

## Constraints (primary/foreign keys)

- PK/FK constraints in Unity Catalog are **informational, NOT ENFORCED** — they
  document intent and let the optimizer make assumptions with `RELY`. They do not
  reject bad data. Say this if a user expects enforcement.
- **PK/FK columns must be `NOT NULL`.** Always declare NOT NULL on key columns.
- A FK must reference a PK or a NOT-NULL UNIQUE key on the parent.
- Syntax:
  ```sql
  ALTER TABLE cat.sch.child ADD CONSTRAINT pk_child PRIMARY KEY (id);
  ALTER TABLE cat.sch.child ADD CONSTRAINT fk_child_parent
    FOREIGN KEY (parent_id) REFERENCES cat.sch.parent (id);
  ```
- To make edits idempotent, `ALTER TABLE … DROP CONSTRAINT IF EXISTS name;` before re-adding.
- Add `RELY` when the data is trusted and you want optimizer benefits:
  `… PRIMARY KEY (id) RELY`.

## Physical design

- **Delta** is the default and only managed format. `USING DELTA`.
- **Liquid clustering** (preferred over partitioning for most tables):
  `CLUSTER BY (col1, col2)` or `CLUSTER BY AUTO`. Do **not** combine with `PARTITIONED BY`.
- Partition only for very large, low-cardinality, time-based access patterns.
- **Surrogate keys**: `id BIGINT GENERATED ALWAYS AS IDENTITY`.
- Add `COMMENT` on tables and columns — it powers search and the ER diagram.
- Governance tags via `TBLPROPERTIES` or `ALTER TABLE … SET TAGS`.

## DML patterns

- **Upsert / SCD-1**:
  ```sql
  MERGE INTO cat.sch.dim_customer t
  USING staging s ON t.customer_id = s.customer_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  ```
- **SCD-2** (history): match on business key + `is_current`, close the old row
  (`UPDATE … SET is_current=false, end_ts=current_timestamp()`), insert the new
  version. Spell out both branches.
- Prefer `INSERT INTO … SELECT` for straightforward loads; `MERGE` for keyed upserts.

## Naming

- medallion: `bronze_<source>`, `silver_<entity>`, `gold_<concept>`.
- star: `fact_<process>`, `dim_<entity>`, `bridge_<a>_<b>`.
- data vault: `hub_<entity>`, `lnk_<relationship>`, `sat_<entity>_<descriptor>`.
