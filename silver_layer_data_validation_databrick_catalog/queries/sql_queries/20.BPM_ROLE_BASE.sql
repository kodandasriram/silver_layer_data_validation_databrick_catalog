WITH bronze_layer AS (
    SELECT
        role.id
		,role.espace_id
		,role.ss_key
		,role.name
		,role.description
		,role.persistent
		,role.is_active as "is_active(business)"
		,defrole.ID AS ACTIVITY_DEF_ROLE_ID
		,defrole.ROLE_ID
		,defrole.ACTIVITY_DEF_ID AS ACTIVITY_DEF
		--,bronze_created_on
		--,bronze_updated_on
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_ROLE as role
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEF_ROLE  as defrole
        ON defrole.role_id = role.id
),

silver_layer AS (

    SELECT 
         id
		,espace_id
		,ss_key
		,name
		,description
		,persistent
		,"is_active(business)"
		,activity_def_role_id
		,role_id
		,activity_def
		--,source_system_name
		--,dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".BPM_ROLE_BASE
    
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

-- 1. DUPLICATE BRONZE
SELECT
    'DUPLICATE_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
)

UNION ALL

-- 2. DUPLICATE SILVER
SELECT
    'DUPLICATE_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
)

UNION ALL

-- 3. PRIMARY KEY NULL CHECK
SELECT
    'PK_NULL_BRONZE',
    COUNT(*),
    NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

SELECT
    'PK_NULL_SILVER',
    COUNT(*),
    NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

-- 4. COLUMN MISMATCH (FIXED AS PER YOUR SELECT)
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
        COALESCE(CAST(b.espace_id AS VARCHAR), '') <> COALESCE(CAST(s.espace_id AS VARCHAR), '')
     OR COALESCE(CAST(b.ss_key AS VARCHAR), '') <> COALESCE(CAST(s.ss_key AS VARCHAR), '')
     OR COALESCE(CAST(b.name AS VARCHAR), '') <> COALESCE(CAST(s.name AS VARCHAR), '')
     OR COALESCE(CAST(b.description AS VARCHAR), '') <> COALESCE(CAST(s.description AS VARCHAR), '')
     OR COALESCE(CAST(b.persistent AS VARCHAR), '') <> COALESCE(CAST(s.persistent AS VARCHAR), '')
     OR COALESCE(CAST(b."is_active(business)" AS VARCHAR), '') <> COALESCE(CAST(s."is_active(business)" AS VARCHAR), '')
     OR COALESCE(CAST(b.activity_def_role_id AS VARCHAR), '') <> COALESCE(CAST(s.activity_def_role_id AS VARCHAR), '')
     OR COALESCE(CAST(b.role_id AS VARCHAR), '') <> COALESCE(CAST(s.role_id AS VARCHAR), '')
     OR COALESCE(CAST(b.activity_def AS VARCHAR), '') <> COALESCE(CAST(s.activity_def AS VARCHAR), '')
) t

UNION ALL

-- 5. BRONZE NOT IN SILVER (SAFE CAST)
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(espace_id AS VARCHAR),
        CAST(ss_key AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(persistent AS VARCHAR),
        CAST("is_active(business)" AS VARCHAR),
        CAST(activity_def_role_id AS VARCHAR),
        CAST(role_id AS VARCHAR),
        CAST(activity_def AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(espace_id AS VARCHAR),
        CAST(ss_key AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(persistent AS VARCHAR),
        CAST("is_active(business)" AS VARCHAR),
        CAST(activity_def_role_id AS VARCHAR),
        CAST(role_id AS VARCHAR),
        CAST(activity_def AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- 6. SILVER NOT IN BRONZE (SAFE CAST)
SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(espace_id AS VARCHAR),
        CAST(ss_key AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(persistent AS VARCHAR),
        CAST("is_active(business)" AS VARCHAR),
        CAST(activity_def_role_id AS VARCHAR),
        CAST(role_id AS VARCHAR),
        CAST(activity_def AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(espace_id AS VARCHAR),
        CAST(ss_key AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(persistent AS VARCHAR),
        CAST("is_active(business)" AS VARCHAR),
        CAST(activity_def_role_id AS VARCHAR),
        CAST(role_id AS VARCHAR),
        CAST(activity_def AS VARCHAR)
    FROM bronze_layer
) t;