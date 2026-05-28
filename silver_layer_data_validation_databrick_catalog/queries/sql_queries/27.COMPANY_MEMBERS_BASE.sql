WITH bronze_layer AS (
    SELECT
        id,
        isadmin,
        verifiedon,
        lastactivity,
        bookinglink
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_COMPANYMEMBERS 
),

silver_layer AS (
   SELECT 
        id,
        isadmin,
        verifiedon,
        lastactivity,
        bookinglink
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".COMPANY_MEMBERS_BASE
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
SELECT
    'DUPLICATE_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- Duplicate Silver
SELECT
    'DUPLICATE_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- ✅ Column Mismatch Count (FIXED)
SELECT
    'COLUMN_MISMATCH_COUNT',
    COUNT(*),
    NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s
        ON b.id = s.id

    WHERE
        COALESCE(CAST(b.isadmin AS VARCHAR), '') 
            <> COALESCE(CAST(s.isadmin AS VARCHAR), '')
     OR COALESCE(CAST(b.verifiedon AS VARCHAR), '') 
            <> COALESCE(CAST(s.verifiedon AS VARCHAR), '')
     OR COALESCE(CAST(b.lastactivity AS VARCHAR), '') 
            <> COALESCE(CAST(s.lastactivity AS VARCHAR), '')
     OR COALESCE(CAST(b.bookinglink AS VARCHAR), '') 
            <> COALESCE(CAST(s.bookinglink AS VARCHAR), '')
) t

UNION ALL

-- Bronze not in Silver
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
) t

UNION ALL

-- Silver not in Bronze
SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;