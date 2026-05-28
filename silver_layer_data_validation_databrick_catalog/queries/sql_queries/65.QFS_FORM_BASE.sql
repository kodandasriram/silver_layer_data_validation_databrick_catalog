WITH bronze_layer AS (
    SELECT
        id,
        CAST(NULL AS VARCHAR) AS domainname,   -- no mapping given
        name,
        description,
        baseurl,
        urlpath,
        isactive,
        'NEO2' AS source_system_name,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updatedat DESC, createdat DESC
               ) AS rnk
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QFS_FORM
    ) t
    WHERE rnk = 1
),

silver_layer AS (
    SELECT
        id,
        domainname,
        name,
        description,
        baseurl,
        urlpath,
        isactive,
        source_system_name,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".QFS_FORM_BASE
)

-- =========================================
-- VALIDATION
-- =========================================

-- 1. COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

-- 3. PK NULL
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

-- 4. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.domainname AS VARCHAR), '') <> COALESCE(CAST(s.domainname AS VARCHAR), '')
     OR COALESCE(CAST(b.name AS VARCHAR), '') <> COALESCE(CAST(s.name AS VARCHAR), '')
     OR COALESCE(CAST(b.description AS VARCHAR), '') <> COALESCE(CAST(s.description AS VARCHAR), '')
     OR COALESCE(CAST(b.baseurl AS VARCHAR), '') <> COALESCE(CAST(s.baseurl AS VARCHAR), '')
     OR COALESCE(CAST(b.urlpath AS VARCHAR), '') <> COALESCE(CAST(s.urlpath AS VARCHAR), '')
     OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
) t

UNION ALL

-- 5. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
) t

UNION ALL

-- 6. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;