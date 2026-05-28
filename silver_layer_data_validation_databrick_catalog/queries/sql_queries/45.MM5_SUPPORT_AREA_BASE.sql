WITH bronze_layer AS (
    SELECT
        code,
        label,
        description,
        arabiclabel,
        iconstyleclass,
        allowaddsupportamendment,
        FALSE AS is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_SUPPORTAREA4
),

silver_layer AS (
    SELECT
        code,
        label,
        description,
        arabiclabel,
        iconstyleclass,
        allowaddsupportamendment
        ---is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".MM5_SUPPORT_AREA_BASE
)

-- =========================
-- VALIDATION
-- =========================

-- 1. COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT code 
    FROM bronze_layer 
    GROUP BY code 
    HAVING COUNT(*) > 1
) t

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT code 
    FROM silver_layer 
    GROUP BY code 
    HAVING COUNT(*) > 1
) t

UNION ALL

-- 3. PK NULL
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE code IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE code IS NULL

UNION ALL

-- 4. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
        CAST(description AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
        CAST(description AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- 5. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
        CAST(description AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(code AS VARCHAR),
        CAST(label AS VARCHAR),
        CAST(description AS VARCHAR)
    FROM bronze_layer
) t

UNION ALL

-- 6. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.code = s.code
    WHERE
        COALESCE(CAST(b.label AS VARCHAR),'') <> COALESCE(CAST(s.label AS VARCHAR),'')
     OR COALESCE(CAST(b.description AS VARCHAR),'') <> COALESCE(CAST(s.description AS VARCHAR),'')
     OR COALESCE(CAST(b.arabiclabel AS VARCHAR),'') <> COALESCE(CAST(s.arabiclabel AS VARCHAR),'')
     OR COALESCE(CAST(b.iconstyleclass AS VARCHAR),'') <> COALESCE(CAST(s.iconstyleclass AS VARCHAR),'')
     OR COALESCE(CAST(b.allowaddsupportamendment AS VARCHAR),'') <> COALESCE(CAST(s.allowaddsupportamendment AS VARCHAR),'')
) t;