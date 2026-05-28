WITH bronze_layer AS (
    SELECT
        id,
        applicationid,
        paymentrequestid,
        eligibilitycriteriarequestty,
        crnumber,
        businessactivitycode,
        descriptionen,
        descriptionar,
        psmonitoringid,
        amendmentrequestid,
        islastversion,
        FALSE AS is_deleted,
        'NEO2' AS source_system_name,
        CAST(updatedon AS TIMESTAMP) AS updatedon,
        CAST(createdon AS TIMESTAMP) AS createdon,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updatedon DESC, createdon DESC
               ) AS rnk
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_MOIC_CRACTIVITY
    ) t
    WHERE rnk = 1
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        paymentrequestid,
        eligibilitycriteriarequestty,
        crnumber,
        businessactivitycode,
        descriptionen,
        descriptionar,
        psmonitoringid,
        amendmentrequestid,
        islastversion,
        is_deleted,
        source_system_name,
        updatedon,
        createdon,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".MYA_MOIC_CRACTIVITY_BASE
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
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- 3. DUPLICATE SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- 4. PK NULL BRONZE
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

-- 5. PK NULL SILVER
SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

-- 6. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.paymentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.paymentrequestid AS VARCHAR), '')
     OR COALESCE(CAST(b.eligibilitycriteriarequestty AS VARCHAR), '') <> COALESCE(CAST(s.eligibilitycriteriarequestty AS VARCHAR), '')
     OR COALESCE(CAST(b.crnumber AS VARCHAR), '') <> COALESCE(CAST(s.crnumber AS VARCHAR), '')
     OR COALESCE(CAST(b.businessactivitycode AS VARCHAR), '') <> COALESCE(CAST(s.businessactivitycode AS VARCHAR), '')
     OR COALESCE(CAST(b.descriptionen AS VARCHAR), '') <> COALESCE(CAST(s.descriptionen AS VARCHAR), '')
     OR COALESCE(CAST(b.descriptionar AS VARCHAR), '') <> COALESCE(CAST(s.descriptionar AS VARCHAR), '')
     OR COALESCE(CAST(b.psmonitoringid AS VARCHAR), '') <> COALESCE(CAST(s.psmonitoringid AS VARCHAR), '')
     OR COALESCE(CAST(b.amendmentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.amendmentrequestid AS VARCHAR), '')
     OR COALESCE(CAST(b.islastversion AS VARCHAR), '') <> COALESCE(CAST(s.islastversion AS VARCHAR), '')
) t

UNION ALL

-- 7. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
) t

UNION ALL

-- 8. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;