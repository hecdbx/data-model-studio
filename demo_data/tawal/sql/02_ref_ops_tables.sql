-- Tawal reference (tawal_ref) + operational (tawal_ops) model with declared
-- informational PK/FK constraints, loaded from the landing volume.
-- Exercises: cross-schema FKs (ops → ref), a composite PK/FK
-- (tower_slots ← leases), two FKs to the same parent (fiber_links → sites),
-- nullable FKs, an FK chain alarms ← work_orders, and a one-to-one extension
-- table (site_power_profiles is keyed by the sites key itself).

-- ── tawal_ref ───────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ref.regions (
  region_id INT NOT NULL COMMENT 'Region surrogate key',
  region_code STRING NOT NULL COMMENT 'Short code used in site ids, e.g. RUH',
  region_name STRING COMMENT 'Region name (English)',
  region_name_ar STRING COMMENT 'Region name (Arabic)',
  population BIGINT COMMENT 'Population (GASTAT estimate)',
  area_km2 INT COMMENT 'Area in square kilometres',
  country_code STRING COMMENT 'ISO country code',
  CONSTRAINT pk_regions PRIMARY KEY (region_id)
) COMMENT 'Administrative regions of the Kingdom of Saudi Arabia';
INSERT INTO hec_demo_catalog.tawal_ref.regions
SELECT CAST(region_id AS INT), region_code, region_name, region_name_ar, CAST(population AS BIGINT),
       CAST(area_km2 AS INT), country_code
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/regions', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ref.cities (
  city_id INT NOT NULL COMMENT 'City surrogate key',
  city_name STRING COMMENT 'City name',
  region_id INT NOT NULL COMMENT 'Region the city belongs to',
  latitude DOUBLE, longitude DOUBLE,
  city_tier STRING COMMENT 'Tier 1 (metro) / Tier 2 / Tier 3 (rural)',
  is_coastal BOOLEAN,
  CONSTRAINT pk_cities PRIMARY KEY (city_id),
  CONSTRAINT fk_cities_region FOREIGN KEY (region_id) REFERENCES hec_demo_catalog.tawal_ref.regions (region_id)
) COMMENT 'Cities and governorates where Tawal operates sites';
INSERT INTO hec_demo_catalog.tawal_ref.cities
SELECT CAST(city_id AS INT), city_name, CAST(region_id AS INT), CAST(latitude AS DOUBLE), CAST(longitude AS DOUBLE),
       city_tier, CAST(is_coastal AS BOOLEAN)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/cities', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ref.tenants (
  tenant_id STRING NOT NULL COMMENT 'Tenant key, e.g. TEN-STC',
  tenant_name STRING COMMENT 'Commercial name',
  tenant_type STRING COMMENT 'MNO, MVNO, ISP, Enterprise, Government, Utility',
  parent_group STRING, credit_rating STRING,
  msa_signed_date DATE COMMENT 'Master service agreement signature date',
  billing_currency STRING, account_manager STRING,
  CONSTRAINT pk_tenants PRIMARY KEY (tenant_id)
) COMMENT 'Customers that lease space on Tawal towers';
INSERT INTO hec_demo_catalog.tawal_ref.tenants
SELECT tenant_id, tenant_name, tenant_type, parent_group, credit_rating, CAST(msa_signed_date AS DATE),
       billing_currency, account_manager
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/tenants', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ref.vendors (
  vendor_id STRING NOT NULL COMMENT 'Vendor key',
  vendor_name STRING, vendor_type STRING COMMENT 'RAN, Antenna, Power, Generator, Tower Fabrication, Fiber, Cooling, Field Services, Solar',
  hq_country STRING, is_local_content BOOLEAN COMMENT 'Counts toward Saudi local-content targets',
  vendor_rating DOUBLE,
  CONSTRAINT pk_vendors PRIMARY KEY (vendor_id)
) COMMENT 'Equipment, fabrication and field-services vendors';
INSERT INTO hec_demo_catalog.tawal_ref.vendors
SELECT vendor_id, vendor_name, vendor_type, hq_country, CAST(is_local_content AS BOOLEAN), CAST(vendor_rating AS DOUBLE)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/vendors', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ref.landlords (
  landlord_id STRING NOT NULL COMMENT 'Landlord key',
  landlord_name STRING, landlord_type STRING COMMENT 'Individual, Company, Government, Municipality',
  national_id_masked STRING, iban_masked STRING, phone STRING,
  CONSTRAINT pk_landlords PRIMARY KEY (landlord_id)
) COMMENT 'Owners of the land or rooftops Tawal sites are built on';
INSERT INTO hec_demo_catalog.tawal_ref.landlords
SELECT landlord_id, landlord_name, landlord_type, national_id_masked, iban_masked, CAST(phone AS STRING)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/landlords', format => 'csv', header => true);

-- ── tawal_ops ───────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.field_teams (
  team_id STRING NOT NULL COMMENT 'Field team key',
  team_name STRING, region_id INT NOT NULL COMMENT 'Region the team covers',
  contractor_id STRING COMMENT 'Field-services vendor employing the team',
  team_lead STRING, headcount INT, shift_pattern STRING, is_active BOOLEAN,
  CONSTRAINT pk_field_teams PRIMARY KEY (team_id),
  CONSTRAINT fk_field_teams_region FOREIGN KEY (region_id) REFERENCES hec_demo_catalog.tawal_ref.regions (region_id),
  CONSTRAINT fk_field_teams_contractor FOREIGN KEY (contractor_id) REFERENCES hec_demo_catalog.tawal_ref.vendors (vendor_id)
) COMMENT 'Outsourced maintenance teams by region';
INSERT INTO hec_demo_catalog.tawal_ops.field_teams
SELECT team_id, team_name, CAST(region_id AS INT), contractor_id, team_lead, CAST(headcount AS INT), shift_pattern,
       CAST(is_active AS BOOLEAN)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/field_teams', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.sites (
  site_id STRING NOT NULL COMMENT 'Site key, e.g. TWL-RUH-0042',
  site_name STRING, city_id INT NOT NULL, landlord_id STRING,
  site_type STRING COMMENT 'Greenfield, Rooftop, Small Cell, In-Building, COW',
  site_status STRING COMMENT 'On-Air, Under Construction, Planned, Decommissioned',
  latitude DOUBLE, longitude DOUBLE, elevation_m INT,
  power_source STRING COMMENT 'Grid, Grid+DG, DG Only, Hybrid Solar-DG, Solar',
  grid_connection_kva INT, commissioned_date DATE, ground_area_sqm INT,
  access_type STRING, is_hub_site BOOLEAN COMMENT 'Aggregation hub for backhaul', cluster_code STRING,
  CONSTRAINT pk_sites PRIMARY KEY (site_id),
  CONSTRAINT fk_sites_city FOREIGN KEY (city_id) REFERENCES hec_demo_catalog.tawal_ref.cities (city_id),
  CONSTRAINT fk_sites_landlord FOREIGN KEY (landlord_id) REFERENCES hec_demo_catalog.tawal_ref.landlords (landlord_id)
) COMMENT 'Physical tower sites — the core asset';
INSERT INTO hec_demo_catalog.tawal_ops.sites
SELECT site_id, site_name, CAST(city_id AS INT), landlord_id, initcap(trim(site_type)), site_status,
       CAST(latitude AS DOUBLE), CAST(longitude AS DOUBLE), CAST(elevation_m AS INT), power_source,
       CAST(grid_connection_kva AS INT), CAST(commissioned_date AS DATE), CAST(ground_area_sqm AS INT), access_type,
       CAST(is_hub_site AS BOOLEAN), cluster_code
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/sites', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.towers (
  tower_id STRING NOT NULL COMMENT 'Tower / structure key',
  site_id STRING NOT NULL,
  structure_type STRING COMMENT 'Lattice, Monopole, Guyed Mast, Camouflaged Palm, Rooftop Pole, Small Cell Pole',
  height_m DOUBLE, max_tenant_slots INT, structural_capacity_kg INT,
  fabricator_vendor_id STRING, build_year INT, last_inspection_date DATE, structural_status STRING,
  CONSTRAINT pk_towers PRIMARY KEY (tower_id),
  CONSTRAINT fk_towers_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_towers_fabricator FOREIGN KEY (fabricator_vendor_id) REFERENCES hec_demo_catalog.tawal_ref.vendors (vendor_id)
) COMMENT 'Passive structures (towers, masts, poles) on each site';
INSERT INTO hec_demo_catalog.tawal_ops.towers
SELECT tower_id, site_id, structure_type, CAST(height_m AS DOUBLE), CAST(max_tenant_slots AS INT),
       CAST(structural_capacity_kg AS INT), fabricator_vendor_id, CAST(build_year AS INT),
       CAST(last_inspection_date AS DATE), structural_status
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/towers', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.tower_slots (
  tower_id STRING NOT NULL, slot_no INT NOT NULL COMMENT 'Mounting position on the tower (1 = top)',
  mount_height_m DOUBLE, azimuth_set STRING, max_load_kg INT,
  slot_status STRING COMMENT 'Occupied, Reserved, Available',
  CONSTRAINT pk_tower_slots PRIMARY KEY (tower_id, slot_no),
  CONSTRAINT fk_tower_slots_tower FOREIGN KEY (tower_id) REFERENCES hec_demo_catalog.tawal_ops.towers (tower_id)
) COMMENT 'Leasable antenna positions on each tower (composite key)';
INSERT INTO hec_demo_catalog.tawal_ops.tower_slots
SELECT tower_id, CAST(slot_no AS INT), CAST(mount_height_m AS DOUBLE), azimuth_set, CAST(max_load_kg AS INT), slot_status
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/tower_slots', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.leases (
  lease_id STRING NOT NULL COMMENT 'Tenancy / site lease agreement key',
  site_id STRING NOT NULL, tower_id STRING NOT NULL, slot_no INT NOT NULL, tenant_id STRING NOT NULL,
  lease_start DATE, lease_end DATE,
  monthly_fee_sar DECIMAL(12,2) COMMENT 'Monthly colocation fee (SAR, excl. VAT)',
  annual_escalation_pct DECIMAL(5,2), technology STRING,
  lease_status STRING COMMENT 'Active, Expired, Pending',
  is_anchor_tenant BOOLEAN, power_included BOOLEAN,
  CONSTRAINT pk_leases PRIMARY KEY (lease_id),
  CONSTRAINT fk_leases_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_leases_slot FOREIGN KEY (tower_id, slot_no) REFERENCES hec_demo_catalog.tawal_ops.tower_slots (tower_id, slot_no),
  CONSTRAINT fk_leases_tenant FOREIGN KEY (tenant_id) REFERENCES hec_demo_catalog.tawal_ref.tenants (tenant_id)
) COMMENT 'Tenant colocation leases on a tower slot';
INSERT INTO hec_demo_catalog.tawal_ops.leases
SELECT lease_id, site_id, tower_id, CAST(slot_no AS INT), tenant_id, CAST(lease_start AS DATE), CAST(lease_end AS DATE),
       CAST(monthly_fee_sar AS DECIMAL(12,2)), CAST(annual_escalation_pct AS DECIMAL(5,2)), technology, lease_status,
       CAST(is_anchor_tenant AS BOOLEAN), CAST(power_included AS BOOLEAN)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/leases', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.lease_invoices (
  invoice_id STRING NOT NULL, lease_id STRING NOT NULL, tenant_id STRING NOT NULL,
  billing_month DATE, amount_sar DECIMAL(12,2), vat_sar DECIMAL(12,2),
  invoice_status STRING COMMENT 'Paid, Open, Overdue, Disputed',
  issued_date DATE, due_date DATE, paid_date DATE,
  CONSTRAINT pk_lease_invoices PRIMARY KEY (invoice_id),
  CONSTRAINT fk_lease_invoices_lease FOREIGN KEY (lease_id) REFERENCES hec_demo_catalog.tawal_ops.leases (lease_id),
  CONSTRAINT fk_lease_invoices_tenant FOREIGN KEY (tenant_id) REFERENCES hec_demo_catalog.tawal_ref.tenants (tenant_id)
) COMMENT 'Monthly colocation invoices issued to tenants';
INSERT INTO hec_demo_catalog.tawal_ops.lease_invoices
SELECT invoice_id, lease_id, tenant_id, CAST(billing_month AS DATE), CAST(amount_sar AS DECIMAL(12,2)),
       CAST(vat_sar AS DECIMAL(12,2)), invoice_status, CAST(issued_date AS DATE), CAST(due_date AS DATE), CAST(paid_date AS DATE)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/lease_invoices', format => 'json');

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.ground_leases (
  ground_lease_id STRING NOT NULL, site_id STRING NOT NULL, landlord_id STRING NOT NULL,
  annual_rent_sar DECIMAL(12,2), start_date DATE, end_date DATE, payment_frequency STRING, has_renewal_option BOOLEAN,
  CONSTRAINT pk_ground_leases PRIMARY KEY (ground_lease_id),
  CONSTRAINT fk_ground_leases_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_ground_leases_landlord FOREIGN KEY (landlord_id) REFERENCES hec_demo_catalog.tawal_ref.landlords (landlord_id)
) COMMENT 'Land / rooftop leases Tawal pays to landlords';
INSERT INTO hec_demo_catalog.tawal_ops.ground_leases
SELECT ground_lease_id, site_id, landlord_id, CAST(annual_rent_sar AS DECIMAL(12,2)), CAST(start_date AS DATE),
       CAST(end_date AS DATE), payment_frequency, CAST(has_renewal_option AS BOOLEAN)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/ground_leases', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.equipment (
  equipment_id STRING NOT NULL, site_id STRING NOT NULL,
  tower_id STRING COMMENT 'Null for ground / shelter equipment',
  lease_id STRING COMMENT 'Tenant lease the equipment belongs to (null = Tawal-owned)',
  vendor_id STRING, equipment_type STRING, model STRING, serial_number STRING,
  ownership STRING COMMENT 'Tawal or Tenant', install_date DATE, warranty_end DATE, equipment_status STRING,
  power_kw DOUBLE, weight_kg DOUBLE,
  CONSTRAINT pk_equipment PRIMARY KEY (equipment_id),
  CONSTRAINT fk_equipment_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_equipment_tower FOREIGN KEY (tower_id) REFERENCES hec_demo_catalog.tawal_ops.towers (tower_id),
  CONSTRAINT fk_equipment_lease FOREIGN KEY (lease_id) REFERENCES hec_demo_catalog.tawal_ops.leases (lease_id),
  CONSTRAINT fk_equipment_vendor FOREIGN KEY (vendor_id) REFERENCES hec_demo_catalog.tawal_ref.vendors (vendor_id)
) COMMENT 'Active and passive equipment installed on sites';
INSERT INTO hec_demo_catalog.tawal_ops.equipment
SELECT equipment_id, site_id, tower_id, lease_id, vendor_id, equipment_type, model, serial_number, ownership,
       CAST(install_date AS DATE), CAST(warranty_end AS DATE), equipment_status, CAST(power_kw AS DOUBLE), CAST(weight_kg AS DOUBLE)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/equipment', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.fiber_links (
  link_id STRING NOT NULL,
  a_site_id STRING NOT NULL COMMENT 'A-end (hub) site', z_site_id STRING NOT NULL COMMENT 'Z-end site',
  link_type STRING COMMENT 'Fiber, Microwave, E-Band', capacity_gbps DOUBLE, distance_km DOUBLE,
  owner_tenant_id STRING COMMENT 'Tenant-owned link (null = Tawal fiber)', commissioned_date DATE, link_status STRING,
  CONSTRAINT pk_fiber_links PRIMARY KEY (link_id),
  CONSTRAINT fk_fiber_links_a_site FOREIGN KEY (a_site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_fiber_links_z_site FOREIGN KEY (z_site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_fiber_links_owner FOREIGN KEY (owner_tenant_id) REFERENCES hec_demo_catalog.tawal_ref.tenants (tenant_id)
) COMMENT 'Backhaul links between sites';
INSERT INTO hec_demo_catalog.tawal_ops.fiber_links
SELECT link_id, a_site_id, z_site_id, link_type, CAST(capacity_gbps AS DOUBLE), CAST(distance_km AS DOUBLE),
       owner_tenant_id, CAST(commissioned_date AS DATE), link_status
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/fiber_links', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.alarms (
  alarm_id STRING NOT NULL, site_id STRING NOT NULL, equipment_id STRING,
  alarm_code STRING, alarm_text STRING, severity STRING COMMENT 'Critical, Major, Minor, Warning',
  raised_at TIMESTAMP, cleared_at TIMESTAMP, source_system STRING,
  CONSTRAINT pk_alarms PRIMARY KEY (alarm_id),
  CONSTRAINT fk_alarms_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_alarms_equipment FOREIGN KEY (equipment_id) REFERENCES hec_demo_catalog.tawal_ops.equipment (equipment_id)
) COMMENT 'NOC alarms raised by OSS / BMS';
INSERT INTO hec_demo_catalog.tawal_ops.alarms
SELECT DISTINCT alarm_id, site_id, equipment_id, alarm_code, alarm_text, initcap(severity),
       CAST(raised_at AS TIMESTAMP), CAST(cleared_at AS TIMESTAMP), source_system
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/alarms', format => 'json');

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.work_orders (
  work_order_id STRING NOT NULL, site_id STRING NOT NULL, equipment_id STRING,
  alarm_id STRING COMMENT 'Alarm that triggered a corrective work order',
  team_id STRING NOT NULL, wo_type STRING, priority STRING COMMENT 'P1 (4h SLA) … P4 (72h SLA)',
  opened_at TIMESTAMP, closed_at TIMESTAMP, wo_status STRING, sla_hours INT, sla_met BOOLEAN,
  labor_hours DOUBLE, cost_sar DECIMAL(12,2),
  CONSTRAINT pk_work_orders PRIMARY KEY (work_order_id),
  CONSTRAINT fk_work_orders_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id),
  CONSTRAINT fk_work_orders_equipment FOREIGN KEY (equipment_id) REFERENCES hec_demo_catalog.tawal_ops.equipment (equipment_id),
  CONSTRAINT fk_work_orders_alarm FOREIGN KEY (alarm_id) REFERENCES hec_demo_catalog.tawal_ops.alarms (alarm_id),
  CONSTRAINT fk_work_orders_team FOREIGN KEY (team_id) REFERENCES hec_demo_catalog.tawal_ops.field_teams (team_id)
) COMMENT 'Field maintenance work orders';
INSERT INTO hec_demo_catalog.tawal_ops.work_orders
SELECT work_order_id, site_id, equipment_id, alarm_id, team_id, wo_type, priority, CAST(opened_at AS TIMESTAMP),
       CAST(closed_at AS TIMESTAMP), wo_status, CAST(sla_hours AS INT), CAST(sla_met AS BOOLEAN),
       CAST(labor_hours AS DOUBLE), CAST(cost_sar AS DECIMAL(12,2))
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/work_orders', format => 'json');

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.energy_readings (
  reading_id BIGINT NOT NULL, site_id STRING NOT NULL, reading_date DATE,
  grid_kwh DOUBLE, dg_kwh DOUBLE COMMENT 'Diesel generator energy', solar_kwh DOUBLE,
  dg_runtime_hours DOUBLE, fuel_level_pct DOUBLE, battery_soc_pct DOUBLE, avg_temp_c DOUBLE,
  power_availability_pct DOUBLE,
  CONSTRAINT pk_energy_readings PRIMARY KEY (reading_id),
  CONSTRAINT fk_energy_readings_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id)
) COMMENT 'Daily site energy telemetry (grid / DG / solar)';
INSERT INTO hec_demo_catalog.tawal_ops.energy_readings
SELECT DISTINCT CAST(reading_id AS BIGINT), site_id, CAST(reading_date AS DATE), CAST(grid_kwh AS DOUBLE),
       CAST(dg_kwh AS DOUBLE), CAST(solar_kwh AS DOUBLE), CAST(dg_runtime_hours AS DOUBLE), CAST(fuel_level_pct AS DOUBLE),
       CAST(battery_soc_pct AS DOUBLE), CAST(avg_temp_c AS DOUBLE), CAST(power_availability_pct AS DOUBLE)
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/energy_readings', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.utility_bills (
  bill_id STRING NOT NULL, site_id STRING NOT NULL, billing_month DATE, meter_no STRING,
  kwh_billed DOUBLE, tariff_sar_per_kwh DECIMAL(6,4), amount_sar DECIMAL(12,2), bill_status STRING,
  CONSTRAINT pk_utility_bills PRIMARY KEY (bill_id),
  CONSTRAINT fk_utility_bills_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id)
) COMMENT 'Saudi Electricity Company (SEC) bills per grid-connected site';
INSERT INTO hec_demo_catalog.tawal_ops.utility_bills
SELECT bill_id, site_id, CAST(billing_month AS DATE), meter_no, CAST(kwh_billed AS DOUBLE),
       CAST(tariff_sar_per_kwh AS DECIMAL(6,4)), CAST(amount_sar AS DECIMAL(12,2)), bill_status
FROM read_files('/Volumes/hec_demo_catalog/tawal_raw/landing/utility_bills', format => 'csv', header => true);

CREATE OR REPLACE TABLE hec_demo_catalog.tawal_ops.site_power_profiles (
  site_id STRING NOT NULL COMMENT 'Same key as sites — one profile per site',
  power_source STRING COMMENT 'Grid, Grid+DG, DG Only, Hybrid Solar-DG, Solar',
  grid_connection_kva INT,
  has_diesel_generator BOOLEAN, has_solar BOOLEAN,
  avg_daily_kwh DOUBLE COMMENT 'Mean daily consumption from energy readings',
  avg_power_availability_pct DOUBLE,
  CONSTRAINT pk_site_power_profiles PRIMARY KEY (site_id),
  CONSTRAINT fk_site_power_profiles_site FOREIGN KEY (site_id) REFERENCES hec_demo_catalog.tawal_ops.sites (site_id)
) COMMENT 'Power setup per site — a one-to-one extension of sites';
INSERT INTO hec_demo_catalog.tawal_ops.site_power_profiles
SELECT s.site_id, s.power_source, s.grid_connection_kva,
       s.power_source LIKE '%DG%', s.power_source LIKE '%Solar%',
       round(e.avg_kwh, 1), round(e.avail, 2)
FROM hec_demo_catalog.tawal_ops.sites s
LEFT JOIN (SELECT site_id,
                  avg(greatest(coalesce(grid_kwh, 0), 0) + coalesce(dg_kwh, 0) + coalesce(solar_kwh, 0)) AS avg_kwh,
                  avg(power_availability_pct) AS avail
           FROM hec_demo_catalog.tawal_ops.energy_readings GROUP BY site_id) e ON e.site_id = s.site_id;

SELECT 'sites' t, count(*) FROM hec_demo_catalog.tawal_ops.sites
UNION ALL SELECT 'leases', count(*) FROM hec_demo_catalog.tawal_ops.leases
UNION ALL SELECT 'alarms', count(*) FROM hec_demo_catalog.tawal_ops.alarms
UNION ALL SELECT 'energy_readings', count(*) FROM hec_demo_catalog.tawal_ops.energy_readings;
