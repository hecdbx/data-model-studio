# Handling edit & change requests

When the user has a model on the canvas (reverse-engineered from Unity Catalog or
hand-built) and asks to change it or generate code:

1. **Read the provided model JSON** as the current state. If it came from UC, the
   entity ids are `schema.table`.
2. **Classify each requested change**:
   - *Additive* (new table, new column, new PK/FK, new comment) → safe. Generate
     `ALTER TABLE … ADD COLUMN`, `CREATE TABLE`, `ADD CONSTRAINT`.
   - *Destructive* (drop column/table, narrow a type, change a key) → call it out
     explicitly, explain the data impact, and provide the statement only if the
     user confirms intent. Never silently drop.
3. **For data movement**, use `MERGE`/`INSERT … SELECT` (see uc_modeling).
4. **Pick the output the user asked for**:
   - "DML" / "SQL" / "DDL" → Unity Catalog SQL. Prefer `generate_sql_ddl` for a
     full rebuild; hand-write `ALTER`/`MERGE` for surgical edits.
   - "pipeline" / "Lakeflow" / "SDP" → update the Spark Declarative Pipeline. If
     the original pipeline source was provided, modify *that* code in place,
     preserving its config block and untouched definitions; otherwise call
     `generate_sdp_pipeline`.
5. **Always** also emit the model *patch* as a ```json block (see system skill) —
   only what you added or changed, plus explicit `removed_entities` /
   `removed_relationships` — so the canvas can apply the change visually.

Keep answers tight: a one-line summary, the code block(s), then the model JSON.
