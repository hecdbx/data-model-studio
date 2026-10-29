"""System-prompt assembly from the embedded skill files, plus the prompts used to
seed a model from a volume."""

from __future__ import annotations

import os
from functools import lru_cache

_SKILLS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "skills")

BASE_SKILLS = ["system", "uc_modeling", "sdp", "editing"]
PATTERN_SKILL = {"medallion": "medallion", "star_schema": "star_schema", "data_vault": "data_vault"}


@lru_cache(maxsize=32)
def load_skill(name: str) -> str:
    path = os.path.join(_SKILLS_DIR, f"{name}.md")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def build_system_prompt(model_type: str | None = None, include_all_patterns: bool = False) -> str:
    parts = [load_skill(s) for s in BASE_SKILLS]
    if include_all_patterns:
        parts += [load_skill(p) for p in ("medallion", "star_schema", "data_vault")]
    elif model_type in PATTERN_SKILL:
        parts.append(load_skill(PATTERN_SKILL[model_type]))
    return "\n\n---\n\n".join(p for p in parts if p)


MODEL_TYPE_INSTRUCTIONS = {
    "medallion": (
        "Design a complete Medallion model. Bronze: one table per source file, raw "
        "structure + _ingested_at, _source_file, _batch_id. Silver: cleansed/typed/"
        "deduped with surrogate + business keys. Gold: business-ready dimensional "
        "tables. Name bronze_<source> / silver_<entity> / gold_<concept>. Keep "
        "descriptions short so the JSON stays valid and complete."
    ),
    "star_schema": (
        "Design a Kimball star schema: fact_<process> tables (FKs + additive measures, "
        "state the grain), dim_<entity> tables (surrogate BIGINT PK, natural key, SCD-2 "
        "where useful), a dim_date, and bridge_<a>_<b> for many-to-many."
    ),
    "data_vault": (
        "Design Data Vault 2.0: hub_<entity> (hash key PK, business key, load_date, "
        "record_source), lnk_<relationship> (hash key, hub hash FKs), sat_<entity>_<desc> "
        "(parent hash FK, load_date, hash_diff, attributes). Use SHA-256 hash keys."
    ),
}


def build_infer_keys_prompt(model_json: str) -> str:
    """One-shot prompt: infer primary keys + foreign-key relationships for a model
    on the canvas. The agent returns a COMPACT diff (not the whole model), which the
    backend applies — this keeps the response small, fast, and within token limits."""
    return f"""You are given a data model's entities and columns as JSON. Infer its **primary keys**
and **foreign-key relationships**, then return them as a COMPACT diff. Do NOT echo the model back.

## How to decide
- **Primary key**: the column(s) that uniquely identify a row — `id`, `<entity>_id`, a
  surrogate `<entity>_sk`, a hub hash key, or the natural business key.
- **Foreign key**: a column that references another entity's key — e.g. `customer_id` →
  `customers`, `account_sk` → `dim_account`, `order_id` → `orders`. The entity holding the
  FK is the child (many); the referenced entity is the parent (one).

## Output — return ONLY this, as a single ```json block, no prose
Use the exact entity **`id`** values given (not names) for every key below.
```json
{{
  "primary_keys": {{ "<entity_id>": ["<pk_col>", "..."] }},
  "foreign_keys": [
    {{ "from_entity": "<child entity_id>", "from_columns": ["<fk_col>"],
       "to_entity": "<parent entity_id>", "to_columns": ["<parent_pk_col>"] }}
  ]
}}
```
Include only entities that have a primary key, and only real foreign keys. Keep it minimal —
the smaller the diff, the better. Do not invent columns that aren't listed.

## Model
```json
{model_json}
```"""


def build_generate_user_prompt(catalog: str, schema: str, volume: str, model_type: str) -> str:
    instruction = MODEL_TYPE_INSTRUCTIONS.get(model_type, MODEL_TYPE_INSTRUCTIONS["medallion"])
    pretty = model_type.replace("_", " ")
    return f"""Analyze the raw data files in a Unity Catalog volume and design a {pretty} data model.

## Volume
- Catalog: {catalog}
- Schema: {schema}
- Volume: {volume}
- Path: /Volumes/{catalog}/{schema}/{volume}

## Requested pattern: {pretty}
{instruction}

## Steps
1. Call `scan_volume` with catalog="{catalog}", schema="{schema}", volume="{volume}" to read every dataset's schema and sample rows.
2. If needed, use `read_file_sample` on individual files for more detail.
3. Identify entities, keys, data types, and relationships from the real data.
4. Return the final model as a single ```json block in the **compact format** below (not the
   canvas schema — the app expands it). Output the JSON block only — no prose before or after it.

## Compact format
```json
{{
  "model_type": "{model_type}",
  "name": "short_model_name",
  "description": "one line",
  "entities": [
    {{"name": "silver_sites", "schema": "optional_schema_or_null", "layer": "silver",
      "description": "short", "pk": ["site_id"],
      "columns": ["site_id STRING", "city_id INT", "commissioned_date DATE", "monthly_fee_sar DECIMAL(12,2)"]}}
  ],
  "relationships": [["silver_sites", ["city_id"], "silver_cities", ["city_id"]]]
}}
```
- `columns` are `"name TYPE"` strings; `pk` lists the primary-key column(s).
- For every table loaded straight from a dataset in the volume, add `"source": "<dataset name
  from the scan, e.g. sites>"` and `"format": "csv|json|parquet"` so the generated pipeline
  ingests the right files.
- `relationships` are `[child_table, [fk_columns], parent_table, [parent_pk_columns]]`, using table names.
- Cover every dataset the scan returned. Keep descriptions under 10 words."""
