WITH bronze_layer AS (
    SELECT
        id,
        COALESCE(TRIM(CAST(isiccode AS VARCHAR)), '') AS isiccode,
        COALESCE(TRIM(CAST(sectorisicactivityid AS VARCHAR)), '') AS sectorisicactivity
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISIC3
),

silver_layer AS (
    SELECT
        id,
        COALESCE(TRIM(CAST(isiccode AS VARCHAR)), '') AS isiccode,
        COALESCE(TRIM(CAST(sectorisicactivity AS VARCHAR)), '') AS sectorisicactivity
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".SECTOR_ISIC_BASE
),

-- =========================================
-- 1. BASIC VALIDATIONS
-- =========================================
count_validation AS (
    SELECT 
        'COUNT_VALIDATION' AS check_name,
        (SELECT COUNT(*) FROM bronze_layer) AS bronze_count,
        (SELECT COUNT(*) FROM silver_layer) AS silver_count
),

duplicate_bronze AS (
    SELECT 'DUPLICATE_BRONZE' AS check_name, COUNT(*) AS cnt
    FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)
),

duplicate_silver AS (
    SELECT 'DUPLICATE_SILVER' AS check_name, COUNT(*) AS cnt
    FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)
),

null_bronze AS (
    SELECT 'PK_NULL_BRONZE' AS check_name, COUNT(*) AS cnt
    FROM bronze_layer WHERE id IS NULL
),

null_silver AS (
    SELECT 'PK_NULL_SILVER' AS check_name, COUNT(*) AS cnt
    FROM silver_layer WHERE id IS NULL
),

-- =========================================
-- 2. ROW LEVEL COMPARISON (REAL FIX)
-- =========================================
comparison AS (
    SELECT
        b.id,

        CASE WHEN COALESCE(b.isiccode,'') = COALESCE(s.isiccode,'')
             THEN 1 ELSE 0 END AS isiccode_match,

        CASE WHEN COALESCE(b.sectorisicactivity,'') = COALESCE(s.sectorisicactivity,'')
             THEN 1 ELSE 0 END AS activity_match

    FROM bronze_layer b
    JOIN silver_layer s
        ON b.id = s.id
),

mismatch_count AS (
    SELECT
        'MISMATCH_COUNT' AS check_name,
        COUNT(*) AS cnt
    FROM comparison
    WHERE isiccode_match = 0 OR activity_match = 0
)

-- =========================================
-- FINAL OUTPUT
-- =========================================
SELECT check_name, bronze_count AS value1, silver_count AS value2
FROM count_validation

UNION ALL

SELECT check_name, cnt, NULL FROM duplicate_bronze
UNION ALL

SELECT check_name, cnt, NULL FROM duplicate_silver
UNION ALL

SELECT check_name, cnt, NULL FROM null_bronze
UNION ALL

SELECT check_name, cnt, NULL FROM null_silver
UNION ALL

SELECT check_name, cnt, NULL FROM mismatch_count;