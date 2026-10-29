# Pattern: Star Schema (Kimball)

- Identify business processes → **fact** tables (`fact_<process>`) holding
  foreign keys to dimensions plus additive numeric measures and a grain
  statement in the description.
- Extract descriptive attributes → **dimension** tables (`dim_<entity>`) with a
  surrogate key (`BIGINT GENERATED ALWAYS AS IDENTITY`) as PK, the natural/business
  key retained, and SCD-2 columns where history matters (`effective_ts`,
  `end_ts`, `is_current`).
- Always include a `dim_date` covering the fact date range.
- Many-to-many → **bridge** tables (`bridge_<a>_<b>`).
- FKs go from fact → dimension surrogate keys. Design for simple BI joins.
