WITH bronze_layer AS (
    SELECT
        code,
        label,
        --"order" AS orders,
        is_active
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISICACTIVITY3
),

silver_layer AS (
    SELECT
        code,
        label,
       -- orders,
        is_active
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".SECTOR_ISIC_ACTIVITY_BASE
)

-- =========================
-- VALIDATION BLOCK
-- =========================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT code FROM bronze_layer GROUP BY code HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT code FROM silver_layer GROUP BY code HAVING COUNT(*) > 1)

UNION ALL

SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE code IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE code IS NULL

UNION ALL

-- BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
       -- CAST(orders AS VARCHAR),
        CAST(is_active AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
        --CAST(orders AS VARCHAR),
        CAST(is_active AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
       --- CAST(orders AS VARCHAR),
        CAST(is_active AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
       --- CAST("order" AS VARCHAR),
        CAST(is_active AS VARCHAR)
    FROM bronze_layer
) t;