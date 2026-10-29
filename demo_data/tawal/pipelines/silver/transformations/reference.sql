-- Silver — reference data (cleansed, typed).

CREATE OR REFRESH MATERIALIZED VIEW silver_regions (
  CONSTRAINT valid_region_id EXPECT (region_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'KSA administrative regions'
AS SELECT
  CAST(region_id AS INT) AS region_id,
  upper(trim(region_code)) AS region_code,
  region_name,
  region_name_ar,
  CAST(population AS BIGINT) AS population,
  CAST(area_km2 AS INT) AS area_km2
FROM hec_demo_catalog.tawal_bronze.bronze_regions;

CREATE OR REFRESH MATERIALIZED VIEW silver_cities (
  CONSTRAINT valid_city_id EXPECT (city_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cities and governorates with their region'
AS SELECT
  CAST(c.city_id AS INT) AS city_id,
  c.city_name,
  CAST(c.region_id AS INT) AS region_id,
  CAST(c.latitude AS DOUBLE) AS latitude,
  CAST(c.longitude AS DOUBLE) AS longitude,
  c.city_tier,
  CAST(c.is_coastal AS BOOLEAN) AS is_coastal
FROM hec_demo_catalog.tawal_bronze.bronze_cities c;

CREATE OR REFRESH MATERIALIZED VIEW silver_tenants
COMMENT 'Tower tenants (MNOs, MVNOs, ISPs, enterprises)'
AS SELECT tenant_id, tenant_name, tenant_type, parent_group, credit_rating,
          CAST(msa_signed_date AS DATE) AS msa_signed_date, billing_currency, account_manager
FROM hec_demo_catalog.tawal_bronze.bronze_tenants;

CREATE OR REFRESH MATERIALIZED VIEW silver_vendors
COMMENT 'Equipment and services vendors'
AS SELECT vendor_id, vendor_name, vendor_type, hq_country,
          CAST(is_local_content AS BOOLEAN) AS is_local_content, CAST(vendor_rating AS DOUBLE) AS vendor_rating
FROM hec_demo_catalog.tawal_bronze.bronze_vendors;

CREATE OR REFRESH MATERIALIZED VIEW silver_landlords
COMMENT 'Site landlords'
AS SELECT landlord_id, landlord_name, landlord_type, national_id_masked, iban_masked, CAST(phone AS STRING) AS phone
FROM hec_demo_catalog.tawal_bronze.bronze_landlords;

CREATE OR REFRESH MATERIALIZED VIEW silver_field_teams
COMMENT 'Field maintenance teams'
AS SELECT team_id, team_name, CAST(region_id AS INT) AS region_id, contractor_id, team_lead,
          CAST(headcount AS INT) AS headcount, shift_pattern, CAST(is_active AS BOOLEAN) AS is_active
FROM hec_demo_catalog.tawal_bronze.bronze_field_teams;
