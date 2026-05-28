WITH bronze_layer AS (
    SELECT
        id,
        sectionid,
        fieldid,
        CAST(rowposition AS BIGINT) AS rowposition,
        CAST(columnposition AS BIGINT) AS columnposition,
        CAST(numbercolumns AS BIGINT) AS numbercolumns,
        isvisible,
        isenabled,
        ismandatory,
        weight,
        hasmanualverification,
        FALSE AS is_deleted,
        'NEO2' AS source_system_name,
        updatedat,
        createdat,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY id 
                ORDER BY updatedat DESC, createdat DESC
            ) AS rnk
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QFS_SECTIONFIELD
    ) t
    WHERE rnk = 1
),

silver_layer AS (
    SELECT
        id,
        sectionid,
        fieldid,
        rowposition,
        columnposition,
        numbercolumns,
        isvisible,
        isenabled,
        ismandatory,
        weight,
        hasmanualverification,
        is_deleted,
        source_system_name,
        updatedat,
        createdat,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".QFS_SECTION_FIELD_BASE
)

-- =========================================
-- VALIDATION
-- =========================================

-- 1. COUNT CHECK
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATE CHECK
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

-- 3. PK NULL CHECK
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
        COALESCE(CAST(b.sectionid AS VARCHAR), '') <> COALESCE(CAST(s.sectionid AS VARCHAR), '')
        OR COALESCE(CAST(b.fieldid AS VARCHAR), '') <> COALESCE(CAST(s.fieldid AS VARCHAR), '')
        OR COALESCE(CAST(b.rowposition AS VARCHAR), '') <> COALESCE(CAST(s.rowposition AS VARCHAR), '')
        OR COALESCE(CAST(b.columnposition AS VARCHAR), '') <> COALESCE(CAST(s.columnposition AS VARCHAR), '')
        OR COALESCE(CAST(b.numbercolumns AS VARCHAR), '') <> COALESCE(CAST(s.numbercolumns AS VARCHAR), '')
        OR COALESCE(CAST(b.isvisible AS VARCHAR), '') <> COALESCE(CAST(s.isvisible AS VARCHAR), '')
        OR COALESCE(CAST(b.isenabled AS VARCHAR), '') <> COALESCE(CAST(s.isenabled AS VARCHAR), '')
        OR COALESCE(CAST(b.ismandatory AS VARCHAR), '') <> COALESCE(CAST(s.ismandatory AS VARCHAR), '')
        OR COALESCE(CAST(b.weight AS VARCHAR), '') <> COALESCE(CAST(s.weight AS VARCHAR), '')
        OR COALESCE(CAST(b.hasmanualverification AS VARCHAR), '') <> COALESCE(CAST(s.hasmanualverification AS VARCHAR), '')
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