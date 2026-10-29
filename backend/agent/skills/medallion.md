# Pattern: Medallion (Bronze → Silver → Gold)

- **Bronze** — one table per source file/stream, raw structure preserved,
  append-only. Add metadata: `_ingested_at TIMESTAMP`, `_source_file STRING`,
  `_batch_id STRING`. No cleansing. Name `bronze_<source>`.
- **Silver** — cleansed, typed, deduplicated, conformed. Enforce data types,
  handle nulls, standardize, add surrogate keys (`BIGINT GENERATED ALWAYS AS
  IDENTITY`) and NOT-NULL business keys. Name `silver_<entity>`.
- **Gold** — business-ready aggregates and a dimensional model (facts + dims)
  for analytics/BI. Optimize with liquid clustering. Name `gold_<concept>`.

Infer relationships from column names and content. Keep entity descriptions
< 20 words and column descriptions < 10 words so JSON stays complete and valid.
