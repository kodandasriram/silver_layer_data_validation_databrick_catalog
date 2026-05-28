WITH bronze_layer AS (

    -- ===============================
    -- NTP APPLICATION STATUS HISTORY
    -- ===============================
    SELECT
        apphis.id,
        apphis.applicationid,
        appstatus.label AS applicationstatus   -- applicationstatusid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONSTATUSHISTORY AS apphis
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_APPLICATIONSTATUS4 AS appstatus
        ON appstatus.code = apphis.applicationstatusid

    UNION ALL

    -- ===============================
    -- PX1 APPLICATION STATUS LOG
    -- ===============================
    SELECT
        applog.id,
        applog.applicationid,
        appsstatus.label AS applicationstatus
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONSTATUSLOG AS applog
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONSTATUS AS appstatus
        ON appstatus.id = applog.originstatus
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONSTATUS AS appsstatus
        ON appsstatus.id = applog.destinationstatus
),

silver_layer AS (
    SELECT 
        id,
        applicationid,
        applicationstatus
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATION_STATUS_HISTORY_BASE
)

-- =========================================

-- ✅ NULL PK CHECK
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

SELECT 'NULL_PK_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

-- ✅ COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- ✅ DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- ✅ COLUMN MISMATCH (CORRECT)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') 
            <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.applicationstatus AS VARCHAR), '') 
            <> COALESCE(CAST(s.applicationstatus AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(applicationstatus AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(applicationstatus AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(applicationstatus AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(applicationstatus AS VARCHAR)
    FROM bronze_layer
) t;