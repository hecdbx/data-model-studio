"""Silver — operations: NOC alarms, work orders, energy telemetry, utility bills."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = spark.conf.get("tawal.bronze_schema", "hec_demo_catalog.tawal_bronze")


@dp.materialized_view(name="silver_alarms", comment="De-duplicated NOC alarms with normalized severity")
@dp.expect_or_drop("valid_alarm_id", "alarm_id IS NOT NULL")
def silver_alarms():
    return (
        spark.read.table(f"{BRONZE}.bronze_alarms")
        .dropDuplicates(["alarm_id"])
        .select(
            "alarm_id", "site_id", "equipment_id", "alarm_code", "alarm_text",
            F.initcap("severity").alias("severity"),
            F.to_timestamp("raised_at").alias("raised_at"),
            F.to_timestamp("cleared_at").alias("cleared_at"),
            "source_system",
        )
    )


@dp.materialized_view(name="silver_work_orders", comment="Work orders with alarm response time")
def silver_work_orders():
    wo = spark.read.table(f"{BRONZE}.bronze_work_orders")
    alarms = spark.read.table(f"{BRONZE}.bronze_alarms").dropDuplicates(["alarm_id"])
    return (
        wo.join(alarms.select("alarm_id", F.to_timestamp("raised_at").alias("alarm_raised_at")), "alarm_id", "left")
        .select(
            "work_order_id", "site_id", "equipment_id", "alarm_id", "team_id", "wo_type", "priority",
            F.to_timestamp("opened_at").alias("opened_at"),
            F.to_timestamp("closed_at").alias("closed_at"),
            "wo_status", F.col("sla_hours").cast("int").alias("sla_hours"), F.col("sla_met").cast("boolean").alias("sla_met"),
            F.col("labor_hours").cast("double").alias("labor_hours"),
            F.col("cost_sar").cast("decimal(12,2)").alias("cost_sar"),
            ((F.unix_timestamp(F.to_timestamp("opened_at")) - F.unix_timestamp("alarm_raised_at")) / 60).alias("response_minutes"),
        )
    )


@dp.materialized_view(name="silver_energy_readings", comment="Daily site energy telemetry (validated, de-duplicated)")
@dp.expect_or_drop("non_negative_grid", "grid_kwh >= 0")
@dp.expect("plausible_temperature", "avg_temp_c BETWEEN -5 AND 60")
def silver_energy_readings():
    return (
        spark.read.table(f"{BRONZE}.bronze_energy_readings")
        .dropDuplicates(["reading_id"])
        .select(
            F.col("reading_id").cast("bigint").alias("reading_id"), "site_id",
            F.to_date("reading_date").alias("reading_date"),
            F.col("grid_kwh").cast("double").alias("grid_kwh"),
            F.col("dg_kwh").cast("double").alias("dg_kwh"),
            F.col("solar_kwh").cast("double").alias("solar_kwh"),
            F.col("dg_runtime_hours").cast("double").alias("dg_runtime_hours"),
            F.col("fuel_level_pct").cast("double").alias("fuel_level_pct"),
            F.col("battery_soc_pct").cast("double").alias("battery_soc_pct"),
            F.col("avg_temp_c").cast("double").alias("avg_temp_c"),
            F.col("power_availability_pct").cast("double").alias("power_availability_pct"),
        )
    )


@dp.materialized_view(name="silver_utility_bills", comment="SEC utility bills for grid-connected sites")
def silver_utility_bills():
    bills = spark.read.table(f"{BRONZE}.bronze_utility_bills")
    sites = spark.read.table(f"{BRONZE}.bronze_sites").select("site_id")
    return bills.join(sites, on="site_id", how="inner").select(
        "bill_id", "site_id", F.to_date("billing_month").alias("billing_month"), "meter_no",
        F.col("kwh_billed").cast("double").alias("kwh_billed"),
        F.col("tariff_sar_per_kwh").cast("decimal(6,4)").alias("tariff_sar_per_kwh"),
        F.col("amount_sar").cast("decimal(12,2)").alias("amount_sar"), "bill_status",
    )
