"""Bronze — raw Tawal landing-zone feeds, ingested incrementally with Auto Loader.

Metadata-driven: one streaming table per entry in FEEDS (feed name -> file format).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("tawal.landing_path", "/Volumes/hec_demo_catalog/tawal_raw/landing")

FEEDS = {
    "regions": "csv",
    "cities": "csv",
    "tenants": "csv",
    "vendors": "csv",
    "landlords": "csv",
    "field_teams": "csv",
    "sites": "csv",
    "towers": "csv",
    "tower_slots": "csv",
    "leases": "csv",
    "lease_invoices": "json",
    "ground_leases": "csv",
    "equipment": "csv",
    "fiber_links": "csv",
    "alarms": "json",
    "work_orders": "json",
    "energy_readings": "csv",
    "utility_bills": "csv",
}


def make_bronze(feed: str, fmt: str):
    @dp.table(
        name=f"bronze_{feed}",
        comment=f"Raw {feed} feed from the Tawal landing zone (Auto Loader, {fmt})",
        table_properties={"quality": "bronze"},
    )
    def bronze():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.inferColumnTypes", "true")
        )
        if fmt == "csv":
            reader = reader.option("header", "true")
        return (
            reader.load(f"{LANDING}/{feed}")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )


for feed, fmt in FEEDS.items():
    make_bronze(feed, fmt)
