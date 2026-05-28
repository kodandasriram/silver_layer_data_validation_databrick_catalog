WITH bronze_layer AS (
    SELECT
	code
	,label
	--,order
	--,isactive
	,isterminalstatus
	,colorid
	--,bronze_created_on
	--,bronze_updated_on
	FROM 
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS
),

silver_layer AS (
   SELECT 
   	code
	,applicaitonstatus as label
	,isterminalstatus
	,colorid
	--is_deleted
	--dbt_updated_on
	--source_system_name
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATION_STATUS_BASE
)

-- =========================================

-- ✅ NULL PRIMARY KEY CHECK (BRONZE)
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE code IS NULL

UNION ALL

-- ✅ NULL PRIMARY KEY CHECK (SILVER)
SELECT 'NULL_PK_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE code IS NULL

UNION ALL

-- COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT code FROM bronze_layer GROUP BY code HAVING COUNT(*) > 1
)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT code FROM silver_layer GROUP BY code HAVING COUNT(*) > 1
)

UNION ALL

-- ✅ COLUMN MISMATCH (FIXED CORRECTLY)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.code = s.code
    WHERE
        COALESCE(CAST(b.label AS VARCHAR), '') <> COALESCE(CAST(s.label AS VARCHAR), '')
     OR COALESCE(CAST(b.isterminalstatus AS VARCHAR), '') <> COALESCE(CAST(s.isterminalstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.colorid AS VARCHAR), '') <> COALESCE(CAST(s.colorid AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        code,
        CAST(label AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR),
        CAST(colorid AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        code,
        CAST(label AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR),
        CAST(colorid AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        code,
        CAST(label AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR),
        CAST(colorid AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        code,
        CAST(label AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR),
        CAST(colorid AS VARCHAR)
    FROM bronze_layer
) t;