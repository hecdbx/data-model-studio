-- Silver — physical assets: sites (enriched with city/region), towers, slots, equipment, links.

CREATE OR REFRESH MATERIALIZED VIEW silver_sites (
  CONSTRAINT valid_site_id EXPECT (site_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_coordinates EXPECT (latitude BETWEEN 15 AND 33 AND longitude BETWEEN 34 AND 56)
)
COMMENT 'Tower sites with standardized site types and region enrichment'
AS SELECT
  s.site_id,
  s.site_name,
  CAST(s.city_id AS INT) AS city_id,
  CAST(c.region_id AS INT) AS region_id,
  upper(r.region_code) AS region_code,
  s.landlord_id,
  initcap(trim(s.site_type)) AS site_type,
  s.site_status,
  CAST(s.latitude AS DOUBLE) AS latitude,
  CAST(s.longitude AS DOUBLE) AS longitude,
  CAST(s.elevation_m AS INT) AS elevation_m,
  s.power_source,
  CAST(s.grid_connection_kva AS INT) AS grid_connection_kva,
  CAST(s.commissioned_date AS DATE) AS commissioned_date,
  CAST(s.ground_area_sqm AS INT) AS ground_area_sqm,
  s.access_type,
  CAST(s.is_hub_site AS BOOLEAN) AS is_hub_site,
  s.cluster_code
FROM hec_demo_catalog.tawal_bronze.bronze_sites s
LEFT JOIN hec_demo_catalog.tawal_bronze.bronze_cities c ON s.city_id = c.city_id
LEFT JOIN hec_demo_catalog.tawal_bronze.bronze_regions r ON c.region_id = r.region_id;

CREATE OR REFRESH MATERIALIZED VIEW silver_towers (
  CONSTRAINT valid_height EXPECT (height_m > 0)
)
COMMENT 'Passive structures per site'
AS SELECT tower_id, site_id, structure_type, CAST(height_m AS DOUBLE) AS height_m,
          CAST(max_tenant_slots AS INT) AS max_tenant_slots, CAST(structural_capacity_kg AS INT) AS structural_capacity_kg,
          fabricator_vendor_id, CAST(build_year AS INT) AS build_year,
          CAST(last_inspection_date AS DATE) AS last_inspection_date, structural_status
FROM hec_demo_catalog.tawal_bronze.bronze_towers;

CREATE OR REFRESH MATERIALIZED VIEW silver_tower_slots
COMMENT 'Leasable antenna positions on each tower'
AS SELECT tower_id, CAST(slot_no AS INT) AS slot_no, CAST(mount_height_m AS DOUBLE) AS mount_height_m,
          azimuth_set, CAST(max_load_kg AS INT) AS max_load_kg, slot_status
FROM hec_demo_catalog.tawal_bronze.bronze_tower_slots;

CREATE OR REFRESH MATERIALIZED VIEW silver_equipment
COMMENT 'Installed active and passive equipment'
AS SELECT e.equipment_id, e.site_id, e.tower_id, e.lease_id, e.vendor_id, e.equipment_type, e.model,
          e.serial_number, e.ownership, CAST(e.install_date AS DATE) AS install_date,
          CAST(e.warranty_end AS DATE) AS warranty_end, e.equipment_status,
          CAST(e.power_kw AS DOUBLE) AS power_kw, CAST(e.weight_kg AS DOUBLE) AS weight_kg
FROM hec_demo_catalog.tawal_bronze.bronze_equipment e
WHERE e.equipment_status <> 'Decommissioned';

CREATE OR REFRESH MATERIALIZED VIEW silver_fiber_links
COMMENT 'Fiber / microwave backhaul links between sites'
AS SELECT link_id, a_site_id, z_site_id, link_type, CAST(capacity_gbps AS DOUBLE) AS capacity_gbps,
          CAST(distance_km AS DOUBLE) AS distance_km, owner_tenant_id,
          CAST(commissioned_date AS DATE) AS commissioned_date, link_status
FROM hec_demo_catalog.tawal_bronze.bronze_fiber_links;
