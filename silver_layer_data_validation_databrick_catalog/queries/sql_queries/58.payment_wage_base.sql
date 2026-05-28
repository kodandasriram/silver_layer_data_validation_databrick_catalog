WITH bronze_layer AS (
    SELECT
        paymentsupportid,
        CAST(monthnumber AS BIGINT) AS monthnumber,
        CAST(year AS BIGINT) AS year,
        CAST(month AS BIGINT) AS month,
        newwage,
        CAST(siodeductions AS DECIMAL(38,8)) AS siodeductions,
        siodeductionsmotives,
        CAST(attendancedeductions AS DECIMAL(38,8)) AS attendancedeductions,
        attendancedeductionsmotives,
        CAST(otherdeductions AS DECIMAL(38,8)) AS otherdeductions,
        otherdeductionsmotives,
        documentinstanceguid,
        wagesupportplanid,
        hastarabutincomeverification,
        CAST(tamkeenshareoverride AS DECIMAL(38,8)) AS tamkeenshareoverride,
        jobcurrentwagestatusmatch,
        updatedon,
        bronze_created_on,
        bronze_updated_on,
        CAST(NULL AS BOOLEAN) AS is_deleted,
        'BRONZE' AS source_system_name,
        CAST(NULL AS TIMESTAMP(6)) AS dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTWAGE
),

silver_layer AS (
    SELECT
        paymentsupportid,
        CAST(monthnumber AS BIGINT) AS monthnumber,
        CAST(year AS BIGINT) AS year,
        CAST(month AS BIGINT) AS month,
        newwage,
        CAST(siodeductions AS DECIMAL(38,8)) AS siodeductions,
        siodeductionsmotives,
        CAST(attendancedeductions AS DECIMAL(38,8)) AS attendancedeductions,
        attendancedeductionsmotives,
        CAST(otherdeductions AS DECIMAL(38,8)) AS otherdeductions,
        otherdeductionsmotives,
        documentinstanceguid,
        wagesupportplanid,
        hastarabutincomeverification,
        CAST(tamkeenshareoverride AS DECIMAL(38,8)) AS tamkeenshareoverride,
        jobcurrentwagestatusmatch,
        updatedon,
        bronze_created_on,
        bronze_updated_on,
        is_deleted,
        source_system_name,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".payment_wage_base
)

-- =========================================
-- VALIDATION
-- =========================================

-- 1. COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATE BRONZE
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT paymentsupportid FROM bronze_layer GROUP BY paymentsupportid HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATE SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT paymentsupportid FROM silver_layer GROUP BY paymentsupportid HAVING COUNT(*) > 1
)

UNION ALL

-- 4. PK NULL CHECK
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE paymentsupportid IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE paymentsupportid IS NULL

UNION ALL

-- 5. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(monthnumber AS VARCHAR),
        CAST(year AS VARCHAR),
        CAST(month AS VARCHAR),
        CAST(newwage AS VARCHAR),
        CAST(siodeductions AS VARCHAR),
        CAST(siodeductionsmotives AS VARCHAR),
        CAST(attendancedeductions AS VARCHAR),
        CAST(attendancedeductionsmotives AS VARCHAR),
        CAST(otherdeductions AS VARCHAR),
        CAST(otherdeductionsmotives AS VARCHAR),
        CAST(documentinstanceguid AS VARCHAR),
        CAST(wagesupportplanid AS VARCHAR),
        CAST(hastarabutincomeverification AS VARCHAR),
        CAST(tamkeenshareoverride AS VARCHAR),
        CAST(jobcurrentwagestatusmatch AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(monthnumber AS VARCHAR),
        CAST(year AS VARCHAR),
        CAST(month AS VARCHAR),
        CAST(newwage AS VARCHAR),
        CAST(siodeductions AS VARCHAR),
        CAST(siodeductionsmotives AS VARCHAR),
        CAST(attendancedeductions AS VARCHAR),
        CAST(attendancedeductionsmotives AS VARCHAR),
        CAST(otherdeductions AS VARCHAR),
        CAST(otherdeductionsmotives AS VARCHAR),
        CAST(documentinstanceguid AS VARCHAR),
        CAST(wagesupportplanid AS VARCHAR),
        CAST(hastarabutincomeverification AS VARCHAR),
        CAST(tamkeenshareoverride AS VARCHAR),
        CAST(jobcurrentwagestatusmatch AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- 6. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(monthnumber AS VARCHAR),
        CAST(year AS VARCHAR),
        CAST(month AS VARCHAR),
        CAST(newwage AS VARCHAR),
        CAST(siodeductions AS VARCHAR),
        CAST(siodeductionsmotives AS VARCHAR),
        CAST(attendancedeductions AS VARCHAR),
        CAST(attendancedeductionsmotives AS VARCHAR),
        CAST(otherdeductions AS VARCHAR),
        CAST(otherdeductionsmotives AS VARCHAR),
        CAST(documentinstanceguid AS VARCHAR),
        CAST(wagesupportplanid AS VARCHAR),
        CAST(hastarabutincomeverification AS VARCHAR),
        CAST(tamkeenshareoverride AS VARCHAR),
        CAST(jobcurrentwagestatusmatch AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(monthnumber AS VARCHAR),
        CAST(year AS VARCHAR),
        CAST(month AS VARCHAR),
        CAST(newwage AS VARCHAR),
        CAST(siodeductions AS VARCHAR),
        CAST(siodeductionsmotives AS VARCHAR),
        CAST(attendancedeductions AS VARCHAR),
        CAST(attendancedeductionsmotives AS VARCHAR),
        CAST(otherdeductions AS VARCHAR),
        CAST(otherdeductionsmotives AS VARCHAR),
        CAST(documentinstanceguid AS VARCHAR),
        CAST(wagesupportplanid AS VARCHAR),
        CAST(hastarabutincomeverification AS VARCHAR),
        CAST(tamkeenshareoverride AS VARCHAR),
        CAST(jobcurrentwagestatusmatch AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- 7. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.paymentsupportid = s.paymentsupportid
    WHERE
        COALESCE(CAST(b.monthnumber AS VARCHAR), '') <> COALESCE(CAST(s.monthnumber AS VARCHAR), '')
     OR COALESCE(CAST(b.year AS VARCHAR), '') <> COALESCE(CAST(s.year AS VARCHAR), '')
     OR COALESCE(CAST(b.month AS VARCHAR), '') <> COALESCE(CAST(s.month AS VARCHAR), '')
     OR COALESCE(CAST(b.newwage AS VARCHAR), '') <> COALESCE(CAST(s.newwage AS VARCHAR), '')
     OR COALESCE(CAST(b.siodeductions AS VARCHAR), '') <> COALESCE(CAST(s.siodeductions AS VARCHAR), '')
     OR COALESCE(CAST(b.attendancedeductions AS VARCHAR), '') <> COALESCE(CAST(s.attendancedeductions AS VARCHAR), '')
     OR COALESCE(CAST(b.otherdeductions AS VARCHAR), '') <> COALESCE(CAST(s.otherdeductions AS VARCHAR), '')
     OR COALESCE(CAST(b.tamkeenshareoverride AS VARCHAR), '') <> COALESCE(CAST(s.tamkeenshareoverride AS VARCHAR), '')
) t;