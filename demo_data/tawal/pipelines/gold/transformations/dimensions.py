"""Gold — conformed dimensions for the Tawal tenancy / revenue / operations star schema."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("tawal.catalog", "hec_demo_catalog")
SILVER = f"{CATALOG}.tawal_silver"


@dp.materialized_view(name="dim_region", comment="Region dimension")
def dim_region():
    return spark.read.table(f"{SILVER}.silver_regions").select(
        F.xxhash64("region_id").alias("region_key"), "region_id", "region_code", "region_name",
        "region_name_ar", "population", "area_km2",
    )


@dp.materialized_view(name="dim_site", comment="Site dimension (site + city + region attributes)")
def dim_site():
    sites = spark.read.table(f"{SILVER}.silver_sites")
    cities = spark.read.table(f"{SILVER}.silver_cities").select("city_id", "city_name", "city_tier", "is_coastal")
    regions = spark.read.table(f"{SILVER}.silver_regions").select("region_id", "region_name")
    return (
        sites.join(cities, "city_id", "left")
        .join(regions, ["region_id"], "left")
        .select(
            F.xxhash64("site_id").alias("site_key"), "site_id", "site_name", "site_type", "site_status",
            "power_source", "access_type", "is_hub_site", "city_id", "city_name", "city_tier", "is_coastal",
            "region_id", "region_code", "region_name", "latitude", "longitude", "commissioned_date",
        )
    )


@dp.materialized_view(name="dim_tenant", comment="Tenant dimension")
def dim_tenant():
    return spark.read.table(f"{SILVER}.silver_tenants").select(
        F.xxhash64("tenant_id").alias("tenant_key"), "tenant_id", "tenant_name", "tenant_type",
        "parent_group", "credit_rating",
    )


@dp.materialized_view(name="dim_vendor", comment="Vendor dimension")
def dim_vendor():
    return spark.read.table(f"{SILVER}.silver_vendors").select(
        F.xxhash64("vendor_id").alias("vendor_key"), "vendor_id", "vendor_name", "vendor_type",
        "hq_country", "is_local_content",
    )


@dp.materialized_view(name="dim_tower", comment="Tower / structure dimension")
def dim_tower():
    towers = spark.read.table(f"{SILVER}.silver_towers")
    site_keys = spark.read.table("dim_site").select("site_id", "site_key")
    return towers.join(site_keys, on="site_id", how="left").select(
        F.xxhash64("tower_id").alias("tower_key"), "tower_id", "site_key", "site_id", "structure_type",
        "height_m", "max_tenant_slots", "structural_capacity_kg", "build_year", "structural_status",
    )


@dp.materialized_view(name="dim_equipment", comment="Equipment dimension with vendor key")
def dim_equipment():
    eq = spark.read.table(f"{SILVER}.silver_equipment")
    vendors = spark.read.table("dim_vendor")
    return eq.join(vendors, eq.vendor_id == vendors.vendor_id, "left").select(
        F.xxhash64(eq.equipment_id).alias("equipment_key"), eq.equipment_id, vendors.vendor_key,
        eq.equipment_type, eq.model, eq.ownership, eq.install_date, eq.equipment_status,
    )


@dp.materialized_view(name="dim_field_team", comment="Field team dimension")
def dim_field_team():
    teams = spark.read.table(f"{SILVER}.silver_field_teams")
    vendors = spark.read.table("dim_vendor").select(
        F.col("vendor_id").alias("_contractor_id"), F.col("vendor_name").alias("contractor_name"))
    return teams.join(vendors, teams["contractor_id"] == F.col("_contractor_id"), "left").select(
        F.xxhash64("team_id").alias("team_key"), "team_id", "team_name", "region_id", "contractor_id",
        "contractor_name", "headcount", "shift_pattern",
    )


@dp.materialized_view(name="dim_date", comment="Calendar dimension (KSA weekend = Friday/Saturday)")
def dim_date():
    days = spark.sql("SELECT explode(sequence(DATE'2017-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
        F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"), F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(6, 7).alias("is_weekend"),
    )
