WITH bronze_layer AS (
    SELECT
         hmy.id
        ,hmy.applicationid
        ,hmy.actions
        ,appaction.label as selectedactionkey
        ,hmy.behaviours
        ,hmy.comment
        ,team.name as team 
        ,hmy.isoverdue
        ,hmy.assignedon
        ,hmy.timeelapsed
        ,hmy.configuredsla
        ,hmy.bronze_updated_on
        ,hmy.bronze_created_on
        ,false as is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_HMY_ACTIVITYEXTENDED hmy
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2FH_APPLICATIONASSESSMENTACTIONS appaction  
        ON appaction.key = hmy.selectedactionkey
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team  
        ON team.id = hmy.teamid 
),

silver_layer AS (
   SELECT 
        id
        ,applicationid
        ,actions
        ,selectedactionkey
        ,behaviours
        ,comment
        ,team
        ,isoverdue
        ,assignedon
        ,timeelapsed
        ,configuredsla
        ,bronze_updated_on
        ,bronze_created_on
        ,is_deleted
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".ACTIVITYEXTENDED_BASE
)

-- =========================================
-- FINAL VALIDATION
-- =========================================

-- 1. COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATES BRONZE
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATES SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

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
WHERE id IS null

UNION ALL

SELECT 
    'PK_DUPLICATE_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT 
    'PK_DUPLICATE_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
)

UNION ALL

-- 4. BRONZE NOT IN SILVER (SAFE CAST)
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(actions AS VARCHAR),
        CAST(selectedactionkey AS VARCHAR),
        CAST(behaviours AS VARCHAR),
        CAST(comment AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(isoverdue AS VARCHAR),
        CAST(assignedon AS VARCHAR),
        CAST(timeelapsed AS VARCHAR),
        CAST(configuredsla AS VARCHAR),
        CAST(bronze_updated_on AS VARCHAR),
        CAST(bronze_created_on AS VARCHAR),
        CAST(is_deleted AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(actions AS VARCHAR),
        CAST(selectedactionkey AS VARCHAR),
        CAST(behaviours AS VARCHAR),
        CAST(comment AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(isoverdue AS VARCHAR),
        CAST(assignedon AS VARCHAR),
        CAST(timeelapsed AS VARCHAR),
        CAST(configuredsla AS VARCHAR),
        CAST(bronze_updated_on AS VARCHAR),
        CAST(bronze_created_on AS VARCHAR),
        CAST(is_deleted AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- 5. SILVER NOT IN BRONZE (SAFE CAST)
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(actions AS VARCHAR),
        CAST(selectedactionkey AS VARCHAR),
        CAST(behaviours AS VARCHAR),
        CAST(comment AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(isoverdue AS VARCHAR),
        CAST(assignedon AS VARCHAR),
        CAST(timeelapsed AS VARCHAR),
        CAST(configuredsla AS VARCHAR),
        CAST(bronze_updated_on AS VARCHAR),
        CAST(bronze_created_on AS VARCHAR),
        CAST(is_deleted AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(actions AS VARCHAR),
        CAST(selectedactionkey AS VARCHAR),
        CAST(behaviours AS VARCHAR),
        CAST(comment AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(isoverdue AS VARCHAR),
        CAST(assignedon AS VARCHAR),
        CAST(timeelapsed AS VARCHAR),
        CAST(configuredsla AS VARCHAR),
        CAST(bronze_updated_on AS VARCHAR),
        CAST(bronze_created_on AS VARCHAR),
        CAST(is_deleted AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- 6. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.actions AS VARCHAR), '') <> COALESCE(CAST(s.actions AS VARCHAR), '')
     OR COALESCE(CAST(b.selectedactionkey AS VARCHAR), '') <> COALESCE(CAST(s.selectedactionkey AS VARCHAR), '')
     OR COALESCE(CAST(b.behaviours AS VARCHAR), '') <> COALESCE(CAST(s.behaviours AS VARCHAR), '')
     OR COALESCE(CAST(b.comment AS VARCHAR), '') <> COALESCE(CAST(s.comment AS VARCHAR), '')
     OR COALESCE(CAST(b.team AS VARCHAR), '') <> COALESCE(CAST(s.team AS VARCHAR), '')
     OR COALESCE(CAST(b.isoverdue AS VARCHAR), '') <> COALESCE(CAST(s.isoverdue AS VARCHAR), '')
     OR COALESCE(CAST(b.assignedon AS VARCHAR), '') <> COALESCE(CAST(s.assignedon AS VARCHAR), '')
     OR COALESCE(CAST(b.timeelapsed AS VARCHAR), '') <> COALESCE(CAST(s.timeelapsed AS VARCHAR), '')
     OR COALESCE(CAST(b.configuredsla AS VARCHAR), '') <> COALESCE(CAST(s.configuredsla AS VARCHAR), '')
     OR COALESCE(CAST(b.bronze_updated_on AS VARCHAR), '') <> COALESCE(CAST(s.bronze_updated_on AS VARCHAR), '')
     OR COALESCE(CAST(b.bronze_created_on AS VARCHAR), '') <> COALESCE(CAST(s.bronze_created_on AS VARCHAR), '')
     OR COALESCE(CAST(b.is_deleted AS VARCHAR), '') <> COALESCE(CAST(s.is_deleted AS VARCHAR), '')
)