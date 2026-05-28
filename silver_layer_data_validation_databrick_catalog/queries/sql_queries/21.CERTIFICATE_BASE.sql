WITH bronze_layer AS (
   SELECT
        id,
        referencecode,
        description,
        createdon,
        updatedon
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_R9T_CERTIFICATE 
),

silver_layer AS (
    SELECT 
        id,
        referencecode,
        description,
        createdon,
        updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".CERTIFICATE_BASE
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

-- ✅ Column Mismatch Count (FIXED FOR CERTIFICATE)
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
        COALESCE(CAST(b.referencecode AS VARCHAR), '') <> COALESCE(CAST(s.referencecode AS VARCHAR), '')
     OR COALESCE(CAST(b.description AS VARCHAR), '') <> COALESCE(CAST(s.description AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
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