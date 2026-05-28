WITH bronze_layer AS (

    SELECT
        id
		,applicationsupportid
		,referencenumber
		,isavailabletoclaim
		--,createdby
		--,createdon
		--,updatedby
		--,updatedon
		--,isactive
		--,bronze_created_on
		--,bronze_updated_on
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTITEMS
),

silver_layer AS (

    SELECT 
        id
		,applicationsupportid
		,referencenumber
		,isavailabletoclaim
		--,code
		--,order
		--,isactive
		--,isvisible
		--,colorid
		--,createdon
		--,updatedon
		--,source_system_name
		--,is_deleted
		--,pricecheckstatus
		--,dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATION_SUPPORT_ITEMS_BASE
)

-- =========================================

-- NULL PK
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'NULL_PK_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

-- COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

-- ✅ COLUMN MISMATCH (CORRECTED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationsupportid AS VARCHAR), '') <> COALESCE(CAST(s.applicationsupportid AS VARCHAR), '')
     OR COALESCE(CAST(b.referencenumber AS VARCHAR), '') <> COALESCE(CAST(s.referencenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.isavailabletoclaim AS VARCHAR), '') <> COALESCE(CAST(s.isavailabletoclaim AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationsupportid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(isavailabletoclaim AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationsupportid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(isavailabletoclaim AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationsupportid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(isavailabletoclaim AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationsupportid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(isavailabletoclaim AS VARCHAR)
    FROM bronze_layer
) t;