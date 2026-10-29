-- Tawal synthetic dataset — schemas + landing volume.
CREATE SCHEMA IF NOT EXISTS hec_demo_catalog.tawal_raw
  COMMENT 'Tawal landing zone: raw CSV / JSON-lines extracts from OSS/BSS, NOC, ERP and SEC billing';
CREATE SCHEMA IF NOT EXISTS hec_demo_catalog.tawal_ref
  COMMENT 'Tawal reference data: regions, cities, tenants (MNOs and enterprises), vendors, landlords';
CREATE SCHEMA IF NOT EXISTS hec_demo_catalog.tawal_ops
  COMMENT 'Tawal operational model (sites, towers, tenancy, field operations, energy) with declared PK/FK constraints';
CREATE SCHEMA IF NOT EXISTS hec_demo_catalog.tawal_bronze
  COMMENT 'Tawal bronze layer: raw ingested feeds (Lakeflow pipeline tawal-bronze-ingest)';
CREATE SCHEMA IF NOT EXISTS hec_demo_catalog.tawal_silver
  COMMENT 'Tawal silver layer: cleansed, typed, de-duplicated entities (Lakeflow pipeline tawal-silver)';
CREATE SCHEMA IF NOT EXISTS hec_demo_catalog.tawal_gold
  COMMENT 'Tawal gold layer: tenancy, revenue, energy and field-ops star schema (Lakeflow pipeline tawal-gold)';
CREATE VOLUME IF NOT EXISTS hec_demo_catalog.tawal_raw.landing
  COMMENT 'Landing zone — one folder per source feed';
