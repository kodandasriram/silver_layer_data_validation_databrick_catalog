WITH bronze_layer AS (
    SELECT
        id,
        programid,
        supportareaid,
        supporttypeid,
        trainingtypeid,
        dimension1,
        dimension2,
        dimension3,
        dimension4,
        FALSE AS is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_INFORDIMENSIONMAPPING
),

silver_layer AS (
    SELECT
        id,
        programid,
        supportareaid,
        supporttypeid,
        trainingtypeid,
        dimension1,
        dimension2,
        dimension3,
        dimension4
        --is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".INFOR_DIMENSION_MAPPING_BASE
)

-- =========================
-- VALIDATION
-- =========================

-- 1. COUNT
SELECT 'COUNT_VALIDATION' AS check_name, COUNT(*) AS bronze_count,
       (SELECT COUNT(*) FROM silver_layer) AS silver_count
FROM bronze_layer

UNION ALL

-- 2. DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- 3. PK NULL
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

-- 4. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(supportareaid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(trainingtypeid AS VARCHAR),
        CAST(dimension1 AS VARCHAR),
        CAST(dimension2 AS VARCHAR),
        CAST(dimension3 AS VARCHAR),
        CAST(dimension4 AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(supportareaid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(trainingtypeid AS VARCHAR),
        CAST(dimension1 AS VARCHAR),
        CAST(dimension2 AS VARCHAR),
        CAST(dimension3 AS VARCHAR),
        CAST(dimension4 AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- 5. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(supportareaid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(trainingtypeid AS VARCHAR),
        CAST(dimension1 AS VARCHAR),
        CAST(dimension2 AS VARCHAR),
        CAST(dimension3 AS VARCHAR),
        CAST(dimension4 AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(supportareaid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(trainingtypeid AS VARCHAR),
        CAST(dimension1 AS VARCHAR),
        CAST(dimension2 AS VARCHAR),
        CAST(dimension3 AS VARCHAR),
        CAST(dimension4 AS VARCHAR)
    FROM bronze_layer
) t

UNION ALL

-- 6. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s
        ON b.id = s.id
    WHERE
        COALESCE(CAST(b.programid AS VARCHAR), '') <> COALESCE(CAST(s.programid AS VARCHAR), '')
        OR COALESCE(CAST(b.supportareaid AS VARCHAR), '') <> COALESCE(CAST(s.supportareaid AS VARCHAR), '')
        OR COALESCE(CAST(b.supporttypeid AS VARCHAR), '') <> COALESCE(CAST(s.supporttypeid AS VARCHAR), '')
        OR COALESCE(CAST(b.trainingtypeid AS VARCHAR), '') <> COALESCE(CAST(s.trainingtypeid AS VARCHAR), '')
        OR COALESCE(CAST(b.dimension1 AS VARCHAR), '') <> COALESCE(CAST(s.dimension1 AS VARCHAR), '')
        OR COALESCE(CAST(b.dimension2 AS VARCHAR), '') <> COALESCE(CAST(s.dimension2 AS VARCHAR), '')
        OR COALESCE(CAST(b.dimension3 AS VARCHAR), '') <> COALESCE(CAST(s.dimension3 AS VARCHAR), '')
        OR COALESCE(CAST(b.dimension4 AS VARCHAR), '') <> COALESCE(CAST(s.dimension4 AS VARCHAR), '')
) t;