"""Gold — facts: tenancy, lease revenue, site energy, work orders, alarms."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("tawal.catalog", "hec_demo_catalog")
SILVER = f"{CATALOG}.tawal_silver"


@dp.materialized_view(name="fact_tenancy", comment="One row per tenant lease on a tower slot (grain: lease)")
def fact_tenancy():
    leases = spark.read.table(f"{SILVER}.silver_leases")
    dsite = spark.read.table("dim_site")
    dtower = spark.read.table("dim_tower")
    dten = spark.read.table("dim_tenant")
    return (
        leases.join(dsite.select("site_id", "site_key"), "site_id", "left")
        .join(dtower.select("tower_id", "tower_key"), "tower_id", "left")
        .join(dten.select("tenant_id", "tenant_key"), "tenant_id", "left")
        .select(
            "lease_id", "site_key", "tower_key", "tenant_key", "slot_no", "technology", "lease_status",
            "is_anchor_tenant", "monthly_fee_sar",
            F.date_format("lease_start", "yyyyMMdd").cast("int").alias("start_date_key"),
            F.date_format("lease_end", "yyyyMMdd").cast("int").alias("end_date_key"),
        )
    )


@dp.materialized_view(name="fact_lease_revenue", comment="Billed colocation revenue (grain: invoice)")
def fact_lease_revenue():
    inv = spark.read.table(f"{SILVER}.silver_lease_invoices")
    lse = spark.read.table(f"{SILVER}.silver_leases")
    dsite = spark.read.table("dim_site")
    dten = spark.read.table("dim_tenant")
    dd = spark.read.table("dim_date")
    return (
        inv.join(lse, inv.lease_id == lse.lease_id, "inner")
        .join(dsite, lse.site_id == dsite.site_id, "left")
        .join(dten, inv.tenant_id == dten.tenant_id, "left")
        .join(dd, F.col("billing_month") == dd.date, "left")
        .select(
            inv.invoice_id, inv.lease_id, dsite.site_key, dten.tenant_key, dd.date_key,
            inv.amount_sar, inv.vat_sar, inv.invoice_status,
            F.datediff(inv.paid_date, inv.issued_date).alias("days_to_pay"),
        )
    )


@dp.materialized_view(name="fact_site_energy_daily", comment="Site energy mix and cost (grain: site x day)")
def fact_site_energy_daily():
    e = spark.read.table(f"{SILVER}.silver_energy_readings")
    b = spark.read.table(f"{SILVER}.silver_utility_bills")
    sites = spark.read.table("dim_site")
    joined = e.join(b, (e.site_id == b.site_id) & (F.trunc(e.reading_date, "MM") == b.billing_month), "left")
    return joined.join(F.broadcast(sites), e.site_id == sites.site_id, "inner").select(
        sites.site_key,
        F.date_format(e.reading_date, "yyyyMMdd").cast("int").alias("date_key"),
        e.grid_kwh, e.dg_kwh, e.solar_kwh,
        (e.grid_kwh + e.dg_kwh + e.solar_kwh).alias("total_kwh"),
        F.round(e.solar_kwh / (e.grid_kwh + e.dg_kwh + e.solar_kwh), 4).alias("renewable_share"),
        e.dg_runtime_hours, e.fuel_level_pct, e.battery_soc_pct, e.power_availability_pct,
        F.round(e.grid_kwh * b.tariff_sar_per_kwh, 2).alias("est_grid_cost_sar"),
    )


@dp.materialized_view(name="fact_work_order", comment="Field work orders with SLA outcome (grain: work order)")
def fact_work_order():
    wo = spark.read.table(f"{SILVER}.silver_work_orders")
    sites = spark.read.table("dim_site").select("site_id", "site_key")
    teams = spark.read.table("dim_field_team").select("team_id", "team_key")
    return (
        wo.join(sites, "site_id")
        .join(teams, "team_id")
        .select(
            "work_order_id", "site_key", "team_key", "alarm_id", "wo_type", "priority", "wo_status",
            F.date_format("opened_at", "yyyyMMdd").cast("int").alias("opened_date_key"),
            "sla_hours", "sla_met", "response_minutes", "labor_hours", "cost_sar",
            ((F.unix_timestamp("closed_at") - F.unix_timestamp("opened_at")) / 3600).alias("resolution_hours"),
        )
    )


@dp.materialized_view(name="fact_alarm", comment="NOC alarms with duration (grain: alarm)")
def fact_alarm():
    al = spark.read.table(f"{SILVER}.silver_alarms")
    sites = spark.read.table("dim_site")
    eq = spark.read.table("dim_equipment")
    return (
        al.join(sites, al.site_id == sites.site_id)
        .join(eq, al.equipment_id == eq.equipment_id, "left")
        .select(
            al.alarm_id, sites.site_key, eq.equipment_key, al.alarm_code, al.severity, al.source_system,
            F.date_format(al.raised_at, "yyyyMMdd").cast("int").alias("raised_date_key"),
            ((F.unix_timestamp(al.cleared_at) - F.unix_timestamp(al.raised_at)) / 60).alias("duration_minutes"),
        )
    )
