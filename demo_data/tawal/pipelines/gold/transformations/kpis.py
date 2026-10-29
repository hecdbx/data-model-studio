"""Gold — KPI marts built with SQL (f-string qualified names + CTEs)."""
from pyspark import pipelines as dp

CATALOG = spark.conf.get("tawal.catalog", "hec_demo_catalog")
G = f"{CATALOG}.tawal_gold"


@dp.materialized_view(name="kpi_tenancy_ratio_by_region", comment="Active tenancies per on-air tower, by region")
def kpi_tenancy_ratio_by_region():
    return spark.sql(f"""
        WITH active AS (
            SELECT tower_key, count(*) AS tenants
            FROM {G}.fact_tenancy
            WHERE lease_status = 'Active'
            GROUP BY tower_key
        )
        SELECT r.region_name,
               count(DISTINCT t.tower_key) AS towers,
               coalesce(sum(a.tenants), 0) AS active_tenancies,
               round(coalesce(sum(a.tenants), 0) / count(DISTINCT t.tower_key), 2) AS tenancy_ratio
        FROM {G}.dim_tower t
        JOIN {G}.dim_site s ON t.site_key = s.site_key
        JOIN {G}.dim_region r ON s.region_id = r.region_id
        LEFT JOIN active a ON a.tower_key = t.tower_key
        WHERE s.site_status = 'On-Air'
        GROUP BY r.region_name
    """)


@dp.materialized_view(name="kpi_monthly_revenue_by_tenant", comment="Billed revenue by tenant and month")
def kpi_monthly_revenue_by_tenant():
    return spark.sql(f"""
        SELECT d.year, d.month, tn.tenant_name, tn.tenant_type,
               sum(f.amount_sar) AS revenue_sar,
               sum(CASE WHEN f.invoice_status = 'Overdue' THEN f.amount_sar ELSE 0 END) AS overdue_sar
        FROM {G}.fact_lease_revenue f
        JOIN {G}.dim_tenant tn ON f.tenant_key = tn.tenant_key
        JOIN {G}.dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month, tn.tenant_name, tn.tenant_type
    """)
