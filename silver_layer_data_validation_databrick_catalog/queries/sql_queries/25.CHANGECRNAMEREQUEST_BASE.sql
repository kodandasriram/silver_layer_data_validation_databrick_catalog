WITH bronze_layer AS (
    SELECT
        id,
        customerid,
        referencenumber,
        remarks,
        changecrnamerequeststatusid,
        oldvalue,
        newvalue,
        newvalue_ar,
        submittedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CHANGECRNAMEREQUEST
),

silver_layer AS (
    SELECT 
        id,
        customerid,
        referencenumber,
        remarks,
        changecrnamerequeststatusid,
        oldvalue,
        newvalue,
        newvalue_ar,
        submittedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".CHANGECRNAMEREQUEST_BASE
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

SELECT
    'COUNT_VALIDATION' AS validation_type,
    COUNT(*) AS bronze_count,
    (SELECT COUNT(*) FROM silver_layer) AS silver_count
FROM bronze_layer

UNION ALL

-- Duplicate Bronze
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
) t

UNION ALL

-- Duplicate Silver
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
) t

UNION ALL

-- ✅ Column Mismatch Count (FIXED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.customerid AS VARCHAR), '') <> COALESCE(CAST(s.customerid AS VARCHAR), '')
     OR COALESCE(CAST(b.referencenumber AS VARCHAR), '') <> COALESCE(CAST(s.referencenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.remarks AS VARCHAR), '') <> COALESCE(CAST(s.remarks AS VARCHAR), '')
     OR COALESCE(CAST(b.changecrnamerequeststatusid AS VARCHAR), '') <> COALESCE(CAST(s.changecrnamerequeststatusid AS VARCHAR), '')
     OR COALESCE(CAST(b.oldvalue AS VARCHAR), '') <> COALESCE(CAST(s.oldvalue AS VARCHAR), '')
     OR COALESCE(CAST(b.newvalue AS VARCHAR), '') <> COALESCE(CAST(s.newvalue AS VARCHAR), '')
     OR COALESCE(CAST(b.newvalue_ar AS VARCHAR), '') <> COALESCE(CAST(s.newvalue_ar AS VARCHAR), '')
     OR COALESCE(CAST(b.submittedon AS VARCHAR), '') <> COALESCE(CAST(s.submittedon AS VARCHAR), '')
) t

UNION ALL

-- Bronze not in Silver
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
) t

UNION ALL

-- Silver not in Bronze
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;