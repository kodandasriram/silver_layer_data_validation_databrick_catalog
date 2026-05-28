WITH bronze_layer AS (
    SELECT
        id,
        applicationid,
        reportingyear,
        applicationmonitoringid,
        year AS perf_year,
        revenue,
        directcost,
        grossprofit,
        overhead,
        netprofit,
        isactive,
        --is_deleted,
        ----source_system_name,
        updatedon,
        createdon
        ----dbt_updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updatedon DESC, createdon DESC
               ) AS rnk
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_OPERATIONPERFORMANCE
    ) t
    WHERE rnk = 1
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        reportingyear,
        applicationmonitoringid,
        perf_year,
        revenue,
        directcost,
        grossprofit,
        overhead,
        netprofit,
        isactive,
        --is_deleted,
       --- source_system_name,
        updatedon,
        createdon
        --dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".OPERATION_PERFORMANCE_BASE
)

-- =========================
-- VALIDATION
-- =========================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT b.id
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.reportingyear AS VARCHAR), '') <> COALESCE(CAST(s.reportingyear AS VARCHAR), '')
     OR COALESCE(CAST(b.perf_year AS VARCHAR), '') <> COALESCE(CAST(s.perf_year AS VARCHAR), '')
     OR COALESCE(CAST(b.revenue AS VARCHAR), '') <> COALESCE(CAST(s.revenue AS VARCHAR), '')
     OR COALESCE(CAST(b.directcost AS VARCHAR), '') <> COALESCE(CAST(s.directcost AS VARCHAR), '')
     OR COALESCE(CAST(b.grossprofit AS VARCHAR), '') <> COALESCE(CAST(s.grossprofit AS VARCHAR), '')
     OR COALESCE(CAST(b.netprofit AS VARCHAR), '') <> COALESCE(CAST(s.netprofit AS VARCHAR), '')
     OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
) t

UNION ALL

SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer
    EXCEPT
    SELECT *
    FROM silver_layer
) t

UNION ALL

SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT *
    FROM silver_layer
    EXCEPT
    SELECT *
    FROM bronze_layer
) t;