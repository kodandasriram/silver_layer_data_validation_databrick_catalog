WITH bronze_layer AS (

    SELECT    
	*
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEFINITION
    
),

silver_layer AS (
    SELECT  
id
,tenant_id
,activity_def_id
,process_id
,activity_name
,user_id
,created
,opened
,closed
,status_id
,is_running_since
,is_running_at
,next_run
,precedent_activity_id
,precedent_outcome
,due_date
,expired
,skipped
,error_count
,inbox_detail
,group_id
,last_error_id
,last_modified
,bronze_created_on
,bronze_updated_on
,activity_definition_id
,ss_key 
,activity_definition_name
,description
,kind
,process_def_id
,is_active
,display_x
,display_y
,invoked_process_def_id
,requires_permission
,inbox_instructions
,skippable
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".BPM_ACTIVITY_BASE
)

-- =========================================
-- VALIDATION OUTPUT
-- =========================================

-- COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- NULL PK
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL FROM bronze_layer WHERE id IS NULL
UNION ALL
SELECT 'NULL_PK_SILVER', COUNT(*), NULL FROM silver_layer WHERE id IS NULL

UNION ALL

-- DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

-- COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
    COALESCE(CAST(b.activity_def_id AS VARCHAR),'') <> COALESCE(CAST(s.activity_def_id AS VARCHAR),'')
 OR COALESCE(CAST(b.process_id AS VARCHAR),'') <> COALESCE(CAST(s.process_id AS VARCHAR),'')
 OR COALESCE(CAST(b.activity_name AS VARCHAR),'') <> COALESCE(CAST(s.activity_name AS VARCHAR),'')
 OR COALESCE(CAST(b.user_id AS VARCHAR),'') <> COALESCE(CAST(s.user_id AS VARCHAR),'')
 OR COALESCE(CAST(b.created AS VARCHAR),'') <> COALESCE(CAST(s.created AS VARCHAR),'')
 OR COALESCE(CAST(b.opened AS VARCHAR),'') <> COALESCE(CAST(s.opened AS VARCHAR),'')
 OR COALESCE(CAST(b.closed AS VARCHAR),'') <> COALESCE(CAST(s.closed AS VARCHAR),'')
 OR COALESCE(CAST(b.status_id AS VARCHAR),'') <> COALESCE(CAST(s.status_id AS VARCHAR),'')
 OR COALESCE(CAST(b.is_running_since AS VARCHAR),'') <> COALESCE(CAST(s.is_running_since AS VARCHAR),'')
 OR COALESCE(CAST(b.is_running_at AS VARCHAR),'') <> COALESCE(CAST(s.is_running_at AS VARCHAR),'')
 OR COALESCE(CAST(b.next_run AS VARCHAR),'') <> COALESCE(CAST(s.next_run AS VARCHAR),'')
 OR COALESCE(CAST(b.precedent_activity_id AS VARCHAR),'') <> COALESCE(CAST(s.precedent_activity_id AS VARCHAR),'')
 OR COALESCE(CAST(b.precedent_outcome AS VARCHAR),'') <> COALESCE(CAST(s.precedent_outcome AS VARCHAR),'')
 OR COALESCE(CAST(b.due_date AS VARCHAR),'') <> COALESCE(CAST(s.due_date AS VARCHAR),'')
 OR COALESCE(CAST(b.expired AS VARCHAR),'') <> COALESCE(CAST(s.expired AS VARCHAR),'')
 OR COALESCE(CAST(b.skipped AS VARCHAR),'') <> COALESCE(CAST(s.skipped AS VARCHAR),'')
 OR COALESCE(CAST(b.error_count AS VARCHAR),'') <> COALESCE(CAST(s.error_count AS VARCHAR),'')
 OR COALESCE(CAST(b.inbox_detail AS VARCHAR),'') <> COALESCE(CAST(s.inbox_detail AS VARCHAR),'')
 OR COALESCE(CAST(b.group_id AS VARCHAR),'') <> COALESCE(CAST(s.group_id AS VARCHAR),'')
 OR COALESCE(CAST(b.last_error_id AS VARCHAR),'') <> COALESCE(CAST(s.last_error_id AS VARCHAR),'')
 OR COALESCE(CAST(b.last_modified AS VARCHAR),'') <> COALESCE(CAST(s.last_modified AS VARCHAR),'')
 OR COALESCE(CAST(b.activity_definition_id AS VARCHAR),'') <> COALESCE(CAST(s.activity_definition_id AS VARCHAR),'')
 OR COALESCE(CAST(b.activity_definition_name AS VARCHAR),'') <> COALESCE(CAST(s.activity_definition_name AS VARCHAR),'')
 OR COALESCE(CAST(b.description AS VARCHAR),'') <> COALESCE(CAST(s.description AS VARCHAR),'')
 OR COALESCE(CAST(b.kind AS VARCHAR),'') <> COALESCE(CAST(s.kind AS VARCHAR),'')
 OR COALESCE(CAST(b.is_active AS VARCHAR),'') <> COALESCE(CAST(s.is_active AS VARCHAR),'')
 OR COALESCE(CAST(b.display_x AS VARCHAR),'') <> COALESCE(CAST(s.display_x AS VARCHAR),'')
 OR COALESCE(CAST(b.display_y AS VARCHAR),'') <> COALESCE(CAST(s.display_y AS VARCHAR),'')
 OR COALESCE(CAST(b.invoked_process_def_id AS VARCHAR),'') <> COALESCE(CAST(s.invoked_process_def_id AS VARCHAR),'')
 OR COALESCE(CAST(b.requires_permission AS VARCHAR),'') <> COALESCE(CAST(s.requires_permission AS VARCHAR),'')
 OR COALESCE(CAST(b.inbox_instructions AS VARCHAR),'') <> COALESCE(CAST(s.inbox_instructions AS VARCHAR),'')
 OR COALESCE(CAST(b.skippable AS VARCHAR),'') <> COALESCE(CAST(s.skippable AS VARCHAR),'')
)

UNION ALL

-- BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
)

UNION ALL

-- SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
);