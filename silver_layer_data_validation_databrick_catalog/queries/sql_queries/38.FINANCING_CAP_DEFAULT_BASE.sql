WITH bronze_layer AS (
    SELECT
        supporttypeid,
        cap,
        createdby,
        createdon,
        updatedby,
        updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_FINANCINGCAPDEFAULT
),

silver_layer AS (
    SELECT
        supporttypeid,
        cap,
        createdby,
        createdon,
        updatedby,
        updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".FINANCING_CAP_DEFAULT_BASE
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
    SELECT supporttypeid 
    FROM bronze_layer 
    GROUP BY supporttypeid 
    HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATE SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT supporttypeid 
    FROM silver_layer 
    GROUP BY supporttypeid 
    HAVING COUNT(*) > 1
)

UNION ALL

-- 4. PK NULL CHECK
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer 
WHERE supporttypeid IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer 
WHERE supporttypeid IS NULL

UNION ALL

-- 5. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(supporttypeid AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(supporttypeid AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- 6. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(supporttypeid AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(supporttypeid AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
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
        ON b.supporttypeid = s.supporttypeid
    WHERE
        COALESCE(CAST(b.cap AS VARCHAR), '') <> COALESCE(CAST(s.cap AS VARCHAR), '')
     OR COALESCE(CAST(b.createdby AS VARCHAR), '') <> COALESCE(CAST(s.createdby AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedby AS VARCHAR), '') <> COALESCE(CAST(s.updatedby AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
) t;