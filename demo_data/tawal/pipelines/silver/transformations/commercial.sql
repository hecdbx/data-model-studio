-- Silver — commercial: tenant leases, invoices, ground leases.

CREATE OR REFRESH MATERIALIZED VIEW silver_leases (
  CONSTRAINT valid_term EXPECT (lease_end >= lease_start) ON VIOLATION DROP ROW,
  CONSTRAINT known_tenant EXPECT (tenant_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Tenant colocation leases'
AS SELECT lease_id, site_id, tower_id, CAST(slot_no AS INT) AS slot_no, tenant_id,
          CAST(lease_start AS DATE) AS lease_start, CAST(lease_end AS DATE) AS lease_end,
          CAST(monthly_fee_sar AS DECIMAL(12,2)) AS monthly_fee_sar,
          CAST(annual_escalation_pct AS DECIMAL(5,2)) AS annual_escalation_pct,
          technology, lease_status, CAST(is_anchor_tenant AS BOOLEAN) AS is_anchor_tenant,
          CAST(power_included AS BOOLEAN) AS power_included
FROM hec_demo_catalog.tawal_bronze.bronze_leases;

CREATE OR REFRESH STREAMING TABLE silver_lease_invoices (
  CONSTRAINT positive_amount EXPECT (amount_sar > 0) ON VIOLATION DROP ROW
)
COMMENT 'Monthly colocation invoices (incremental)'
AS SELECT invoice_id, lease_id, tenant_id, CAST(billing_month AS DATE) AS billing_month,
          CAST(amount_sar AS DECIMAL(12,2)) AS amount_sar, CAST(vat_sar AS DECIMAL(12,2)) AS vat_sar,
          invoice_status, CAST(issued_date AS DATE) AS issued_date, CAST(due_date AS DATE) AS due_date,
          CAST(paid_date AS DATE) AS paid_date
FROM STREAM(hec_demo_catalog.tawal_bronze.bronze_lease_invoices);

CREATE OR REFRESH MATERIALIZED VIEW silver_ground_leases
COMMENT 'Ground / rooftop leases paid to landlords'
AS SELECT g.ground_lease_id, g.site_id, g.landlord_id, CAST(g.annual_rent_sar AS DECIMAL(12,2)) AS annual_rent_sar,
          CAST(g.start_date AS DATE) AS start_date, CAST(g.end_date AS DATE) AS end_date,
          g.payment_frequency, CAST(g.has_renewal_option AS BOOLEAN) AS has_renewal_option
FROM hec_demo_catalog.tawal_bronze.bronze_ground_leases AS g
JOIN hec_demo_catalog.tawal_bronze.bronze_landlords AS l ON g.landlord_id = l.landlord_id;
